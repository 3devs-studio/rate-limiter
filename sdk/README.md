# SDKs

Packages that give an application fair-share rate limiting **without the
limiter service in the request path**. Each one talks to Redis directly and
runs the same Lua scripts as the service (`internal/limiter/scripts`), against
the same keys, so an app using an SDK and an app behind the service share one
set of buckets and get the same answers.

```
                      HTTP API                              SDK
app ──POST /v1/acquire──▶ limiter service ──EVALSHA──▶ Redis     app(SDK) ──EVALSHA──▶ Redis
```

What an SDK does **not** do is proxy mode (store a request, replay it later,
absorb the third party's 429). That needs long-running workers and stays in
the service. The two can run side by side on one Redis.

## Status

| Language | Package | Framework adapter | Conformance suite |
|---|---|---|---|
| [Go](go/) | `github.com/3devs-studio/rate-limiter/sdk/go` (`fairshare`) | `net/http` middleware | passes |
| [TypeScript](typescript/) | `fairshare-client` (ioredis) | Hono, Express | passes |
| [Python](python/) | `fairshare` (redis-py, sync + asyncio) | ASGI (FastAPI, Starlette) | passes |
| [Rust](rust/) | `fairshare` crate (redis-rs, tokio) | axum (feature `axum`) | passes |
| [Ruby](ruby/) | `fairshare` gem (redis-rb) | Rack (Rails) | passes |
| [PHP](php/) | `fairshare/client` (Predis) | PSR-15, Laravel | written, not yet executed |
| [Java](java/) | `io.fairshare:fairshare-client` (Jedis) | Jakarta servlet filter (Spring Boot) | written, not yet executed |
| [Kotlin](kotlin/) | `fairshare-ktor` (over the Java core) | Ktor plugin, suspend wrappers | written, not yet executed |

"Passes" means the package ran [`conformance/cases.json`](conformance/cases.json)
against a real Redis on the authoring machine. PHP, Java and Kotlin ship the
same test but no toolchain was available to run it; run `vendor/bin/phpunit`,
`mvn test` or `gradle test` before relying on them.

## The contract every SDK implements

| Operation | Meaning |
|---|---|
| `new(redis, upstream)` | One instance per upstream. Shares are read from the config key in Redis and cached for 10 s. |
| `try_acquire(service, cost = 1)` | One decision, never waits. Returns `allowed`, `mode` (`own`, `borrow`, `share`, `global`, `cooldown`), `retry_after`, `global_tokens`, `service_tokens`. |
| `acquire(service, cost = 1, wait = 0)` | Retries with the returned `retry_after` until granted or `wait` elapses. Adds `waited`. This is the queue in SDK form. |
| `report_rate_limited(retry_after)` | The upstream answered 429: zero the global bucket and block everyone until then. |
| `sync_remaining(n)` | Lower the global bucket to the upstream's reported remaining quota. Never raises it. |
| `reset()` | Delete the buckets so every share reads as full. Demos and tests. |
| `publish(redis, upstream, spec)` | Write the shares to Redis when no service runs. |
| `shares_from_weights(rate, burst, weights)` | Derive shares from weights the way the service does. |
| framework adapter | Resolve the calling service (header by default), `acquire` with a wait, pass with `X-Fair-Share-*` headers or refuse with 429 + `Retry-After`. Fails open when Redis is unreachable or the service is unknown. |

Errors every SDK distinguishes: *unknown service* (not in the published
config) and *no config* (nothing published for the upstream).

## Where the config comes from

The service publishes each upstream's resolved shares to
`rl:{<upstream>}:config` at startup (or with `ratelimiter -publish -config x.yaml`
to publish and exit):

```json
{"schema": 1, "upstream": "thirdparty", "rate": 90, "burst": 10,
 "services": {"my-app": {"weight": 1, "rate": 90, "burst": 10}},
 "published_at": "2026-10-18T12:00:00Z", "published_by": "ratelimiter"}
```

SDKs read that document, never the YAML, so weights cannot drift between
languages. Without a service, call the SDK's `publish` from the one process
that owns the configuration.

## Keeping the SDKs in step

- `scripts/sync-scripts.sh` copies the Lua from `internal/limiter/scripts`
  into every SDK (verbatim files for Ruby, PHP, Java, Rust; generated string
  constants for TypeScript and Python). Run it after editing a script and
  commit the result. Go imports the service's package directly.
- `conformance/cases.json` is the behaviour contract: burst split by share,
  borrowing that leaves the others' reserve, cost, cooldown, quota sync,
  blocking acquire, reset, unknown service. Every SDK's test suite interprets
  it; add a case there and all eight suites pick it up.
- The config document carries `schema: 1`. Bump it together with any
  incompatible change to the Lua or the key layout, and make the SDKs refuse
  unknown schemas, as they do now.

## Which to use

- **Your own apps, Redis reachable**: the SDK. One Redis round trip, no extra
  service to run.
- **Third parties integrating with you, or apps that must not hold Redis
  credentials**: keep them behind the service (proxy mode) or the HTTP acquire
  API. au1h does this per application, see its `examples/rate-limit-demo`.
- **Both at once** is fine: the service, the dashboard's stats endpoint and
  every SDK read and write the same buckets.
