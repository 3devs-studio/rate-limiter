<?php

declare(strict_types=1);

namespace Fairshare;

/** An upstream's budget and the shares of its services. */
final class Spec
{
    /** @param array<string, Share> $services */
    public function __construct(
        public readonly float $rate,
        public readonly float $burst,
        public readonly array $services,
    ) {
    }

    /** Split rate and burst by weight, the way the service does. @param array<string, float> $weights */
    public static function fromWeights(float $rate, float $burst, array $weights): self
    {
        $sum = array_sum($weights);
        $services = [];
        foreach ($weights as $name => $w) {
            $services[$name] = new Share($rate * $w / $sum, max(1.0, $burst * $w / $sum), $w);
        }
        return new self($rate, $burst, $services);
    }
}
