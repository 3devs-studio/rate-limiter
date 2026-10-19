<?php

declare(strict_types=1);

namespace Fairshare;

use Predis\ClientInterface;
use Predis\Response\ServerException;

/**
 * Fair-share rate limiting straight from Redis.
 *
 * The decision logic is the Lua in src/scripts, identical to what the rate
 * limiter service runs, so an app using this package and an app behind the
 * service share the same buckets and the same answers.
 *
 *   $limiter = new FairShare(new \Predis\Client($_ENV['REDIS_URL']), 'thirdparty');
 *   $d = $limiter->acquire('my-app', 1, 4000);
 *   if (!$d->allowed) { // back off for $d->retryAfterMs }
 */
final class FairShare
{
    public const CONFIG_SCHEMA = 1;

    /** @var array<string, string> script name => sha */
    private array $shas = [];
    private ?Spec $spec = null;
    private float $loadedAt = 0.0;

    public function __construct(
        private readonly ClientInterface $redis,
        public readonly string $upstream,
        private readonly float $configTtl = 10.0,
    ) {
    }

    public static function configKey(string $upstream): string
    {
        return "rl:{{$upstream}}:config";
    }

    private function globalKey(): string
    {
        return "rl:{{$this->upstream}}:g";
    }

    private function serviceKey(string $service): string
    {
        return "rl:{{$this->upstream}}:s:$service";
    }

    /** The upstream's shares as published to Redis (cached). */
    public function spec(): Spec
    {
        if ($this->spec !== null && microtime(true) - $this->loadedAt < $this->configTtl) {
            return $this->spec;
        }
        try {
            $raw = $this->redis->get(self::configKey($this->upstream));
        } catch (\Predis\PredisException $e) {
            if ($this->spec !== null) {
                return $this->spec; // Redis hiccup: keep what we have
            }
            throw $e;
        }
        if ($raw === null) {
            throw new NoConfigException($this->upstream);
        }
        $doc = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        if (($doc['schema'] ?? null) !== self::CONFIG_SCHEMA) {
            throw new FairshareException(sprintf('published config for %s has schema %s, expected %d', $this->upstream, $doc['schema'] ?? 'none', self::CONFIG_SCHEMA));
        }
        $services = [];
        foreach ($doc['services'] as $name => $s) {
            $services[$name] = new Share((float) $s['rate'], (float) $s['burst'], isset($s['weight']) ? (float) $s['weight'] : null);
        }
        $this->spec = new Spec((float) $doc['rate'], (float) $doc['burst'], $services);
        $this->loadedAt = microtime(true);
        return $this->spec;
    }

    /** Ask once; never waits. */
    public function tryAcquire(string $service, float $cost = 1.0): Decision
    {
        $spec = $this->spec();
        $me = $spec->services[$service] ?? throw new UnknownServiceException($service);
        // Same KEYS/ARGV layout as the service: global, mine, then the others by name.
        $keys = [$this->globalKey(), $this->serviceKey($service)];
        $args = [$cost, $spec->rate, $spec->burst, $me->rate, $me->burst];
        $names = array_keys($spec->services);
        sort($names);
        foreach ($names as $name) {
            if ($name === $service) {
                continue;
            }
            $keys[] = $this->serviceKey($name);
            $args[] = $spec->services[$name]->rate;
            $args[] = $spec->services[$name]->burst;
        }
        [$allowed, $retryMs, $mode, $g, $s] = $this->run('acquire', $keys, $args);
        return new Decision((int) $allowed === 1, (string) $mode, (int) $retryMs, 0, (float) $g, (float) $s);
    }

    /** Keep asking until a token is granted or $waitMs has elapsed. */
    public function acquire(string $service, float $cost = 1.0, int $waitMs = 0): Decision
    {
        $start = microtime(true);
        $d = $this->tryAcquire($service, $cost);
        while (!$d->allowed) {
            $remainingMs = $waitMs - (microtime(true) - $start) * 1000;
            if ($remainingMs <= 0) {
                break;
            }
            usleep((int) (min(max($d->retryAfterMs, 5), $remainingMs, 1000) * 1000));
            $d = $this->tryAcquire($service, $cost);
        }
        return $d->withWaited((int) ((microtime(true) - $start) * 1000));
    }

    /** The upstream answered 429: every service backs off for $retryAfterMs. */
    public function reportRateLimited(int $retryAfterMs): void
    {
        $spec = $this->spec();
        $this->run('cooldown', [$this->globalKey()], [max(1, $retryAfterMs), $spec->rate, $spec->burst]);
    }

    /** Lower the global bucket to what the upstream says is left. Never raises it. */
    public function syncRemaining(float $remaining): void
    {
        $spec = $this->spec();
        $this->run('sync', [$this->globalKey()], [$remaining, $spec->rate, $spec->burst]);
    }

    /** Refill every bucket. For demos and tests. */
    public function reset(): void
    {
        $keys = [$this->globalKey()];
        foreach (array_keys($this->spec()->services) as $name) {
            $keys[] = $this->serviceKey($name);
        }
        $this->redis->del($keys);
    }

    /** Write an upstream's shares to Redis without running the service. */
    public static function publish(ClientInterface $redis, string $upstream, Spec $spec, string $by = 'fairshare-php'): void
    {
        $services = [];
        foreach ($spec->services as $name => $s) {
            $services[$name] = array_filter(['rate' => $s->rate, 'burst' => $s->burst, 'weight' => $s->weight], fn ($v) => $v !== null);
        }
        $doc = [
            'schema' => self::CONFIG_SCHEMA,
            'upstream' => $upstream,
            'rate' => $spec->rate,
            'burst' => $spec->burst,
            'services' => $services,
            'published_at' => gmdate('Y-m-d\TH:i:s\Z'),
            'published_by' => $by,
        ];
        $redis->set(self::configKey($upstream), json_encode($doc, JSON_THROW_ON_ERROR));
    }

    /** EVALSHA with a one-time SCRIPT LOAD when Redis does not know the script yet. @return mixed */
    private function run(string $name, array $keys, array $args): mixed
    {
        $this->shas[$name] ??= (string) $this->redis->script('load', self::script($name));
        $argv = array_map(static fn ($a) => is_float($a) ? rtrim(rtrim(sprintf('%.10F', $a), '0'), '.') : (string) $a, $args);
        try {
            return $this->redis->evalsha($this->shas[$name], count($keys), ...$keys, ...$argv);
        } catch (ServerException $e) {
            if (!str_contains($e->getMessage(), 'NOSCRIPT')) {
                throw $e;
            }
            $this->shas[$name] = (string) $this->redis->script('load', self::script($name));
            return $this->redis->evalsha($this->shas[$name], count($keys), ...$keys, ...$argv);
        }
    }

    private static function script(string $name): string
    {
        static $cache = [];
        return $cache[$name] ??= file_get_contents(__DIR__ . "/scripts/$name.lua");
    }
}
