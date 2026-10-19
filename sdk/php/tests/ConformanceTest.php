<?php

declare(strict_types=1);

namespace Fairshare\Tests;

use Fairshare\FairShare;
use Fairshare\Share;
use Fairshare\Spec;
use Fairshare\UnknownServiceException;
use PHPUnit\Framework\TestCase;
use Predis\Client;

/** Runs sdk/conformance/cases.json against a real Redis (REDIS_ADDR, default localhost:6379). */
final class ConformanceTest extends TestCase
{
    private static array $suite;
    private static Client $redis;

    public static function setUpBeforeClass(): void
    {
        self::$suite = json_decode(file_get_contents(__DIR__ . '/../../conformance/cases.json'), true, 512, JSON_THROW_ON_ERROR);
        [$host, $port] = explode(':', getenv('REDIS_ADDR') ?: 'localhost:6379');
        self::$redis = new Client(['host' => $host, 'port' => (int) $port, 'timeout' => 1]);
        try {
            self::$redis->ping();
        } catch (\Throwable $e) {
            self::markTestSkipped("no Redis: {$e->getMessage()}");
        }
    }

    public static function cases(): iterable
    {
        $suite = json_decode(file_get_contents(__DIR__ . '/../../conformance/cases.json'), true, 512, JSON_THROW_ON_ERROR);
        foreach ($suite['cases'] as $case) {
            yield $case['name'] => [$case];
        }
    }

    /** @dataProvider cases */
    public function testCase(array $case): void
    {
        $tol = self::$suite['tolerance'] ?? 0.1;
        $upstream = 'conformance-php-' . $case['name'];
        $services = [];
        foreach ($case['spec']['services'] as $name => $s) {
            $services[$name] = new Share((float) $s['rate'], (float) $s['burst']);
        }
        FairShare::publish(self::$redis, $upstream, new Spec((float) $case['spec']['rate'], (float) $case['spec']['burst'], $services));
        $lim = new FairShare(self::$redis, $upstream);
        $lim->reset();
        foreach ($case['steps'] as $i => $step) {
            $where = "{$case['name']} step $i";
            switch ($step['op']) {
                case 'acquire':
                    $e = $step['expect'];
                    $d = $lim->acquire($step['service'], (float) ($step['cost'] ?? 1), (int) ($step['wait_ms'] ?? 0));
                    self::assertSame($e['allowed'], $d->allowed, $where);
                    if (isset($e['mode'])) {
                        self::assertSame($e['mode'], $d->mode, $where);
                    }
                    if (isset($e['service_tokens'])) {
                        self::assertEqualsWithDelta($e['service_tokens'], $d->serviceTokens, $tol, $where);
                    }
                    if (isset($e['global_tokens'])) {
                        self::assertEqualsWithDelta($e['global_tokens'], $d->globalTokens, $tol, $where);
                    }
                    if (isset($e['min_waited_ms'])) {
                        self::assertGreaterThanOrEqual($e['min_waited_ms'], $d->waitedMs, $where);
                    }
                    if (isset($e['min_retry_after_ms'])) {
                        self::assertGreaterThanOrEqual($e['min_retry_after_ms'], $d->retryAfterMs, $where);
                    }
                    if (isset($e['max_retry_after_ms'])) {
                        self::assertLessThanOrEqual($e['max_retry_after_ms'], $d->retryAfterMs, $where);
                    }
                    break;
                case 'drain':
                    $count = 0;
                    $last = '';
                    while ($count < ($step['max'] ?? 1000)) {
                        $d = $lim->tryAcquire($step['service'], 1.0);
                        $last = $d->mode;
                        if (!$d->allowed) {
                            break;
                        }
                        $count++;
                    }
                    self::assertSame($step['expect_count'], $count, $where);
                    if (isset($step['expect_last_mode'])) {
                        self::assertSame($step['expect_last_mode'], $last, $where);
                    }
                    break;
                case 'acquire_error':
                    $this->expectException(UnknownServiceException::class);
                    $lim->tryAcquire($step['service'], 1.0);
                    break;
                case 'cooldown':
                    $lim->reportRateLimited((int) $step['ms']);
                    break;
                case 'sync':
                    $lim->syncRemaining((float) $step['remaining']);
                    break;
                case 'sleep':
                    usleep((int) $step['ms'] * 1000);
                    break;
                case 'reset':
                    $lim->reset();
                    break;
                default:
                    self::fail("unknown op {$step['op']}");
            }
        }
    }
}
