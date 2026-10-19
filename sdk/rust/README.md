# fairshare (Rust)

Fair-share rate limiting from Redis, no limiter service in the request path.
Same Lua, same keys, same answers as the service; shares are read from the
config the service publishes (or `fairshare::publish`).

```rust
let client = redis::Client::open("redis://127.0.0.1/")?;
let limiter = fairshare::FairShare::connect(&client, "thirdparty").await?;

let d = limiter.acquire("my-app", 1.0, Duration::from_secs(4)).await?;
if !d.allowed { /* back off for d.retry_after */ }
```

axum (feature `axum`):

```rust
use axum::middleware::from_fn_with_state;
use fairshare::axum::Gate;
let gate = Gate::new(limiter).service_from_header("x-service").wait(Duration::from_secs(4)).shared();
let app = Router::new()
    .route("/api/me", get(me))
    .layer(from_fn_with_state(gate, fairshare::axum::middleware));
```

| Method | What |
|---|---|
| `try_acquire(service, cost)` | One decision, never waits |
| `acquire(service, cost, wait)` | Retries until granted or `wait` elapses; `waited` says how long |
| `report_rate_limited(retry_after)` | The upstream answered 429: everyone backs off |
| `sync_remaining(n)` | Lower the global bucket to the upstream's reported remaining quota |
| `reset()` | Refill every bucket |
| `publish(&mut conn, upstream, &spec)` | Write the shares to Redis when no service runs |
| `shares_from_weights(rate, burst, &weights)` | Derive shares from weights like the service |

Test: `cargo test --all-features` (needs Redis at `REDIS_ADDR`, default `localhost:6379`).
