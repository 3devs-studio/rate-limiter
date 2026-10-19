package io.fairshare;

import java.time.Duration;

/** The outcome of an acquire. mode is own, borrow, share, global or cooldown. */
public record Decision(boolean allowed, String mode, Duration retryAfter, Duration waited, double globalTokens, double serviceTokens) {
    Decision withWaited(Duration waited) {
        return new Decision(allowed, mode, retryAfter, waited, globalTokens, serviceTokens);
    }
}
