# fairshare-client (Java)

Fair-share rate limiting from Redis, no limiter service in the request path.
Same Lua, same keys, same answers as the service; shares are read from the
config the service publishes (or `FairShare.publish`).

```java
FairShare limiter = new FairShare(new JedisPooled("localhost", 6379), "thirdparty");

Decision d = limiter.acquire("my-app", 1, Duration.ofSeconds(4));   // retries until granted or 4 s
if (!d.allowed()) {
    throw new IllegalStateException("rate limited, retry in " + d.retryAfter().toMillis() + " ms");
}
```

Spring Boot / any Jakarta servlet container:

```java
@Bean
FilterRegistrationBean<FairShareFilter> fairShare(FairShare limiter) {
    var reg = new FilterRegistrationBean<>(new FairShareFilter(limiter, r -> r.getHeader("X-Service"), Duration.ofSeconds(4)));
    reg.addUrlPatterns("/api/*");
    return reg;
}
```

| Method | What |
|---|---|
| `tryAcquire(service, cost)` | One decision, never waits |
| `acquire(service, cost, wait)` | Retries until granted or `wait` elapses; `waited()` says how long |
| `reportRateLimited(retryAfter)` | The upstream answered 429: everyone backs off |
| `syncRemaining(n)` | Lower the global bucket to the upstream's reported remaining quota |
| `reset()` | Refill every bucket |
| `FairShare.publish(redis, upstream, spec)` | Write the shares to Redis when no service runs |
| `Spec.fromWeights(rate, burst, weights)` | Derive shares from weights like the service |

Test: `mvn test` (needs Redis at `REDIS_ADDR`, default `localhost:6379`).

Status: written against the shared conformance suite but not yet executed (no JDK on the authoring machine).
