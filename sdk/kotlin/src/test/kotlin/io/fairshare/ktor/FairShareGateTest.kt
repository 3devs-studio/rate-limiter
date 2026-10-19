package io.fairshare.ktor

import io.fairshare.FairShare
import io.fairshare.Share
import io.fairshare.Spec
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.install
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import redis.clients.jedis.JedisPooled
import kotlin.test.Test
import kotlin.test.assertEquals

/** Needs Redis at REDIS_ADDR (default localhost:6379). The admission logic itself is covered by the Java conformance test. */
class FairShareGateTest {
    @Test
    fun gateAdmitsThenRefuses() = testApplication {
        val (host, port) = (System.getenv("REDIS_ADDR") ?: "localhost:6379").split(":")
        val redis = JedisPooled(host, port.toInt())
        FairShare.publish(redis, "conformance-ktor", Spec(1.0, 2.0, mapOf("web" to Share(1.0, 2.0))))
        val limiter = FairShare(redis, "conformance-ktor").also { it.reset() }

        application {
            install(FairShareGate) { this.limiter = limiter }
            routing { get("/api/x") { call.respondText("ok") } }
        }
        val codes = (1..3).map { client.get("/api/x") { header("X-Service", "web") }.status }
        assertEquals(listOf(HttpStatusCode.OK, HttpStatusCode.OK, HttpStatusCode.TooManyRequests), codes)
        assertEquals(HttpStatusCode.OK, client.get("/api/x").status) // no service header: unmanaged
    }
}
