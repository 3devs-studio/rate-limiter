<?php

declare(strict_types=1);

namespace Fairshare;

/** One service's share of the upstream. */
final class Share
{
    public function __construct(
        public readonly float $rate,
        public readonly float $burst,
        public readonly ?float $weight = null,
    ) {
    }
}
