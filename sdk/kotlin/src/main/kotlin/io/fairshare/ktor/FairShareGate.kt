package io.fairshare.ktor

import io.fairshare.FairShare
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.request.header
import io.ktor.server.response.header
import io.ktor.server.response.respondText
import java.time.Duration

/** Configuration for [FairShareGate]. */
class FairShareGateConfig {
    /** The limiter for this upstream. Required. */
    lateinit var limiter: FairShare

    /** Which service is calling. Return null to let the request through unmanaged. */
    var service: (ApplicationCall) -> String? = { it.request.header("X-Service") }

    /** How long a request may wait for a token before 429. */
    var wait: Duration = Duration.ZERO
    var cost: Double = 1.0

    /** Let requests through when Redis is down or the service is unknown (default true). */
    var failOpen: Boolean = true
}

/**
 * Ktor plugin: admit or refuse incoming requests on behalf of the service that sent them.
 *
 * ```kotlin
 * install(FairShareGate) {
 *     limiter = FairShare(JedisPooled("localhost", 6379), "thirdparty")
 *     service = { call -> call.request.header("X-Service") }
 *     wait = Duration.ofSeconds(4)
 * }
 * ```
 */
val FairShareGate = createApplicationPlugin("FairShareGate", ::FairShareGateConfig) {
    val cfg = pluginConfig
    onCall { call ->
        val service = cfg.service(call) ?: return@onCall
        val decision = try {
            cfg.limiter.acquireSuspend(service, cfg.cost, cfg.wait)
        } catch (e: RuntimeException) {
            if (!cfg.failOpen) {
                call.respondText("rate limiter unavailable", status = HttpStatusCode.ServiceUnavailable)
            } else {
                call.response.header("X-Fair-Share-Mode", "unavailable")
            }
            return@onCall
        }
        call.response.header("X-Fair-Share-Service", service)
        call.response.header("X-Fair-Share-Mode", decision.mode)
        call.response.header("X-Fair-Share-Wait-Ms", decision.waited.toMillis().toString())
        if (!decision.allowed) {
            call.response.header("Retry-After", maxOf(1L, (decision.retryAfter.toMillis() + 999) / 1000).toString())
            call.respondText(
                """{"error":"Too Many Requests","service":"$service","mode":"${decision.mode}","retryAfterMs":${decision.retryAfter.toMillis()}}""",
                ContentType.Application.Json,
                HttpStatusCode.TooManyRequests,
            )
        }
    }
}
