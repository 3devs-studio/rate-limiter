# fairshare (Python)

Fair-share rate limiting from Redis, no limiter service in the request path.
Same Lua, same keys, same answers as the service; shares are read from the
config the service publishes (or `fairshare.publish`).

```python
import redis
from fairshare import FairShare

limiter = FairShare(redis.Redis.from_url(REDIS_URL), "thirdparty")

d = limiter.acquire("my-app", wait=4.0)          # retries until granted or 4 s
if not d.allowed:
    raise RuntimeError(f"rate limited, retry in {d.retry_after_ms} ms")
```

Async and ASGI (FastAPI, Starlette, Quart):

```python
import redis.asyncio as aredis
from fairshare.aio import AsyncFairShare
from fairshare.asgi import FairShareMiddleware, service_from_header

limiter = AsyncFairShare(aredis.from_url(REDIS_URL), "thirdparty")
app.add_middleware(FairShareMiddleware, limiter=limiter, service=service_from_header("x-service"), wait=4.0)
```

| Method | What |
|---|---|
| `try_acquire(service, cost=1)` | One decision, never waits |
| `acquire(service, cost=1, wait=0)` | Retries until granted or `wait` seconds; `waited_ms` says how long |
| `report_rate_limited(retry_after)` | The upstream answered 429: everyone backs off |
| `sync_remaining(n)` | Lower the global bucket to the upstream's reported remaining quota |
| `reset()` | Refill every bucket |
| `publish(redis, upstream, Spec(...))` | Write the shares to Redis when no service runs |
| `shares_from_weights(rate, burst, weights)` | Derive shares from weights like the service |

Test: `uv run --extra test pytest -q` (needs Redis at `REDIS_ADDR`, default `localhost:6379`).
