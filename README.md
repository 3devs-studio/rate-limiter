# ratelimiter

A shared rate-limit gateway for third-party APIs.

When several services (M1, M2, M3, ...) call the same third-party API, each
one only sees its own traffic. If the API allows 100 requests/second and M1 is
already sending 70 while M2 sends 30, M3 gets 429s through no fault of its own.

This service sits between your services and the third party and:

- keeps **one global token bucket in Redis** that mirrors the third party's limit,
  so every limiter instance and every service shares the same view of what is left;
- gives **each service a guaranteed share** (weighted token bucket) and lends
  unused share to busier services without ever starving the quiet ones;
- **queues requests in Redis** when no token is free and replays them as tokens
  refill, so callers get the third party's real answer instead of a 429;
- if the third party still answers 429, applies a **global cooldown** from
  `Retry-After` and retries the request from the queue. Services never see it.

```
   M1 ─┐                         ┌──────────────────────────┐
   M2 ─┼─ /v1/proxy/thirdparty ─▶│ ratelimiter               │──▶ third-party API
   M3 ─┘                         │  admit now  ──────────────┼──▶ (100 r/s)
                                 │  else queue ──▶ workers ──┼──▶
                                 └─────────┬────────────────┘
                                           │ Redis: buckets + queues + results
```

## Quick start

```bash
make build
make upstream          # terminal 1: fake third-party API limited to 100 r/s
make run               # terminal 2: the limiter (needs Redis on localhost:6379)
make loadgen           # terminal 3: m1, m2, m3 demand 140 r/s in total
```

or with Docker:

```bash
make docker-demo
```

A typical run against a 100 r/s upstream with services demanding 140 r/s:

```
service  sent  200  429  other  queued  retried  p50     p95     max
m1       699   699  0    0      691     0        5.549s  6.722s  8.096s
m2       300   300  0    0      281     0        322ms   1.992s  3.92s
m3       400   400  0    0      393     0        1.836s  2.929s  4.216s

1399 requests, 1399 ok, 0 rate limited as seen by services
third-party API saw: accepted=1399 rejected_429=0 peak_in_window=98 (limit 100 per 1s)
```

m1 asked for far more than its share, so its requests waited longest. m2
stayed inside its share and got near-immediate answers. Nobody saw a 429.

## How admission works

Every upstream has a global bucket (`rate`, `burst`) and one bucket per
service sized by `weight / sum(weights)`. One Lua script (atomic in Redis)
decides each request:

1. **Cooldown?** If the third party told us to back off, refuse with the time left.
2. **Global empty?** Refuse. The third party's limit is the hard ceiling.
3. **Within own share?** Take a token from both the service and global buckets.
4. **Over own share?** Borrow from the global pool only if what remains still
   covers every other service's current unused tokens. A quiet service keeps
   its full reserve; a busy one gets everything the others are not using.

So with weights 4/3/3 on 90 r/s, m1 is guaranteed 36 r/s, but when m2 and m3
are idle, m1 can use the whole 90 r/s.

### Picking `rate` and `burst`

A token bucket admits at most `burst + rate * T` requests in any `T` seconds.
Most third parties count requests in a window, so keep
`rate * window + burst <= their limit`. For "100 per second" use
`rate: 90, burst: 10` (the example config), not `rate: 100, burst: 100`.

If the third party reports remaining quota in a header, set `remaining_header`
and the global bucket is lowered to match after every response.

## API

All proxy calls need `X-Service: <name>`; the name must be configured.

| Call | What it does |
|---|---|
| `ANY /v1/proxy/{upstream}/{path}` | Forwards the request to `base_url + path` (method, headers, query and body preserved). Returns the upstream's response. If it had to queue, waits up to `X-Wait` (default `default_wait`), then answers `202` with an id to poll. |
| `GET /v1/requests/{id}` | The stored upstream response for a queued request, or `202` while still queued, `404` once expired. |
| `POST /v1/acquire/{upstream}` `{"service":"m1","cost":1,"wait":"5s"}` | For services that call the third party themselves: `200 {"allowed":true,"mode":"own"}` or `429` with `Retry-After` and `retry_after_ms`. With `wait`, the call is held until a token is free or the wait elapses, so a caller gets the queue without proxying its request body. |
| `POST /v1/upstreams/{upstream}/reset` | Refills every bucket. For demos and tests. |
| `POST /v1/upstreams/{upstream}/cooldown` `{"retry_after_ms":1000}` | A service that got a 429 directly tells everyone to back off. |
| `GET /v1/upstreams/{upstream}/stats` | Token levels, cooldown, queue depth and blocking acquires waiting, per service. |
| `GET /healthz` | Liveness. |

Optional proxy headers: `X-Cost` (tokens this call is worth, default 1),
`X-Wait` (`0s` for fire-and-poll), `X-Callback-Url` (POSTed the result JSON when done).

Response headers added by the limiter: `X-RateLimiter-Request-Id`,
`X-RateLimiter-Queued`, `X-RateLimiter-Wait-Ms`, `X-RateLimiter-Attempts`.

Error mapping: `400` bad input, `403` unknown service, `503` + `Retry-After`
when the service's queue is full, `502` when the upstream kept rejecting or
failed, `504` when a request waited longer than `request_ttl`.

Example:

```bash
curl -H 'X-Service: m1' -H 'X-Wait: 5s' http://localhost:8080/v1/proxy/thirdparty/items/42
```

## Configuration

See [config.example.yaml](config.example.yaml), the full design in [docs/DESIGN.md](docs/DESIGN.md), and per-language snippets in [docs/INTEGRATION.md](docs/INTEGRATION.md). `${VAR}` and `${VAR:-default}`
are expanded from the environment.

## Layout

- `cmd/ratelimiter` main binary
- `internal/limiter` token buckets and Lua scripts (`scripts/acquire.lua` is the heart of it)
- `internal/store` request/result persistence, per-service queues, result notifications
- `internal/engine` admit-or-queue logic, queue workers, 429 handling
- `internal/forwarder` replays captured requests against the real upstream
- `internal/server` HTTP API
- `examples/fake-upstream` a strict third-party API stand-in
- `examples/loadgen` simulates several services

## SDKs

Applications can also skip the HTTP hop and run the same Lua against Redis
through a package: Go, TypeScript, Python, Rust, Ruby, PHP, Java and Kotlin
SDKs live in [sdk/](sdk/README.md), with one middleware each and a shared
conformance suite. The service publishes each upstream's shares to Redis at
startup (`-publish` publishes and exits), which is what the SDKs read.

## Running more than one limiter instance

All state is in Redis, so instances can be scaled horizontally behind a load
balancer. Bucket keys share a hash tag per upstream (`rl:{thirdparty}:...`) so
Redis Cluster works. `concurrency` is per instance; total in-flight upstream
calls are `concurrency * instances`, still capped by the shared bucket.

## Known limits

- A request a worker has popped from its queue is lost if that limiter
  instance crashes before storing the result. Callers waiting on it get a
  `202` after `X-Wait` and a `404` on poll once `request_ttl` has passed.
- Queued requests are replayed as captured; make sure the third party calls
  you queue are safe to be delayed by up to `request_ttl`.
- Redis 7+ (or any Redis that allows `TIME` inside scripts) is required.

## Tests

```bash
make test
```

Unit tests run against an in-memory Redis (miniredis) and cover share
splitting, borrowing fairness, the global ceiling, cooldown, quota sync,
queueing, 429 recovery, async submit and poll, and input validation.
