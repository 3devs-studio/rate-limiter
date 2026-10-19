package io.fairshare.ktor

import io.fairshare.Decision
import io.fairshare.FairShare
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.time.Duration

/**
 * Coroutine-friendly wrappers over the Java core. The Jedis calls block, so
 * they run on Dispatchers.IO instead of the request's event loop thread.
 */
suspend fun FairShare.tryAcquireSuspend(service: String, cost: Double = 1.0): Decision =
    withContext(Dispatchers.IO) { tryAcquire(service, cost) }

suspend fun FairShare.acquireSuspend(service: String, cost: Double = 1.0, wait: Duration = Duration.ZERO): Decision =
    withContext(Dispatchers.IO) { acquire(service, cost, wait) }

suspend fun FairShare.reportRateLimitedSuspend(retryAfter: Duration) =
    withContext(Dispatchers.IO) { reportRateLimited(retryAfter) }
