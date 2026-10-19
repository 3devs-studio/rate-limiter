# fairshare/client (PHP)

Fair-share rate limiting from Redis, no limiter service in the request path.
Same Lua, same keys, same answers as the service; shares are read from the
config the service publishes (or `FairShare::publish`).

```php
use Fairshare\FairShare;

$limiter = new FairShare(new \Predis\Client($_ENV['REDIS_URL']), 'thirdparty');

$d = $limiter->acquire('my-app', 1, 4000);     // retries until granted or 4 s
if (!$d->allowed) {
    throw new \RuntimeException("rate limited, retry in {$d->retryAfterMs} ms");
}
```

PSR-15 (Slim, Mezzio):

```php
$app->add(new \Fairshare\Psr15Middleware($limiter, $responseFactory,
    service: fn ($req) => $req->getHeaderLine('x-service') ?: null, waitMs: 4000));
```

Laravel: register `Fairshare\LaravelMiddleware` with an alias and bind a
`FairShare` per upstream as `fairshare.<upstream>`; see the class docblock.

| Method | What |
|---|---|
| `tryAcquire($service, $cost = 1.0)` | One decision, never waits |
| `acquire($service, $cost = 1.0, $waitMs = 0)` | Retries until granted or the wait elapses; `waitedMs` says how long |
| `reportRateLimited($retryAfterMs)` | The upstream answered 429: everyone backs off |
| `syncRemaining($n)` | Lower the global bucket to the upstream's reported remaining quota |
| `reset()` | Refill every bucket |
| `FairShare::publish($redis, $upstream, $spec)` | Write the shares to Redis when no service runs |
| `Spec::fromWeights($rate, $burst, $weights)` | Derive shares from weights like the service |

Test: `composer install && vendor/bin/phpunit` (needs Redis at `REDIS_ADDR`, default `localhost:6379`).

Status: written against the shared conformance suite but not yet executed (no PHP toolchain on the authoring machine).
