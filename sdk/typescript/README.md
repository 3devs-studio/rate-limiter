# fairshare-client (TypeScript)

Fair-share rate limiting from Redis, no limiter service in the request path.
Same Lua, same keys, same answers as the service; the shares are read from the
config the service publishes (or `FairShare.publish`).

```ts
import Redis from "ioredis";
import { FairShare } from "fairshare-client";
import { fairShare } from "fairshare-client/hono";

const limiter = new FairShare(new Redis(process.env.REDIS_URL!), "thirdparty");

// Gate incoming callers (Hono; `fairshare-client/express` for Express):
app.use("/api/*", fairShare(limiter, { service: (c) => c.req.header("x-service"), waitMs: 4000 }));

// Or guard an outbound call:
const d = await limiter.acquire("my-app", 1, 4000);
if (!d.allowed) throw new Error(`rate limited, retry in ${d.retryAfterMs}ms`);
```

| Method | What |
|---|---|
| `tryAcquire(service, cost=1)` | One decision, never waits |
| `acquire(service, cost=1, waitMs=0)` | Retries until granted or the wait elapses; `waitedMs` says how long |
| `reportRateLimited(retryAfterMs)` | The upstream answered 429: everyone backs off |
| `syncRemaining(n)` | Lower the global bucket to the upstream's reported remaining quota |
| `reset()` | Refill every bucket (demos, tests) |
| `FairShare.publish(redis, upstream, spec)` | Write the shares to Redis when no service runs |
| `FairShare.sharesFromWeights(rate, burst, weights)` | Derive shares from weights like the service |

Test: `bun test` (needs Redis at `REDIS_ADDR`, default `localhost:6379`; runs `sdk/conformance/cases.json`).
