# fairshare-ktor (Kotlin)

Kotlin surface over the Java core (`../java`, `io.fairshare:fairshare-client`):
suspend wrappers that keep Jedis off the event loop, and a Ktor plugin.

```kotlin
val limiter = FairShare(JedisPooled("localhost", 6379), "thirdparty")

// Ktor
install(FairShareGate) {
    this.limiter = limiter
    service = { call -> call.request.header("X-Service") }
    wait = Duration.ofSeconds(4)
}

// Or guard an outbound call from a coroutine
val d = limiter.acquireSuspend("my-app", wait = Duration.ofSeconds(4))
if (!d.allowed) error("rate limited, retry in ${d.retryAfter.toMillis()} ms")
```

The admission API is the Java one (`tryAcquire`, `acquire`, `reportRateLimited`,
`syncRemaining`, `reset`, `FairShare.publish`, `Spec.fromWeights`); the `*Suspend`
extensions wrap the blocking calls in `Dispatchers.IO`.

Build: `cd ../java && mvn install`, then `gradle test` here (needs Redis at `REDIS_ADDR`).

Status: written but not yet executed (no JDK, Gradle or Kotlin toolchain on the authoring machine).
