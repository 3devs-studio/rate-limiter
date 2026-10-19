<?php

declare(strict_types=1);

namespace Fairshare;

/** The outcome of an acquire. */
final class Decision
{
    public function __construct(
        public readonly bool $allowed,
        /** own, borrow, share, global or cooldown */
        public readonly string $mode,
        public readonly int $retryAfterMs,
        public readonly int $waitedMs,
        public readonly float $globalTokens,
        public readonly float $serviceTokens,
    ) {
    }

    public function withWaited(int $waitedMs): self
    {
        return new self($this->allowed, $this->mode, $this->retryAfterMs, $waitedMs, $this->globalTokens, $this->serviceTokens);
    }
}
