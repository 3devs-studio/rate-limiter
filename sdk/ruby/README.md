# fairshare (Ruby / Rails)

Fair-share rate limiting from Redis, no limiter service in the request path.
Same Lua, same keys, same answers as the service; shares are read from the
config the service publishes (or `Fairshare.publish`).

```ruby
limiter = Fairshare::Limiter.new(Redis.new(url: ENV["REDIS_URL"]), "thirdparty")

d = limiter.acquire("my-app", wait: 4)            # retries until granted or 4 s
raise "rate limited, retry in #{d.retry_after_ms} ms" unless d.allowed
```

Rails (Rack middleware):

```ruby
# config/initializers/fairshare.rb
LIMITER = Fairshare::Limiter.new(Redis.new(url: ENV["REDIS_URL"]), "thirdparty")
Rails.application.config.middleware.use Fairshare::Rack,
  limiter: LIMITER, service: ->(env) { env["HTTP_X_SERVICE"] }, wait: 4
```

| Method | What |
|---|---|
| `try_acquire(service, cost = 1)` | One decision, never waits |
| `acquire(service, cost: 1, wait: 0)` | Retries until granted or `wait` seconds; `waited_ms` says how long |
| `report_rate_limited(seconds)` | The upstream answered 429: everyone backs off |
| `sync_remaining(n)` | Lower the global bucket to the upstream's reported remaining quota |
| `reset` | Refill every bucket |
| `Fairshare.publish(redis, upstream, rate:, burst:, services:)` | Write the shares to Redis when no service runs |
| `Fairshare.shares_from_weights(rate, burst, weights)` | Derive shares from weights like the service |

Test: `ruby -Ilib test/conformance_test.rb` (needs the `redis` gem and Redis at `REDIS_ADDR`, default `localhost:6379`).
