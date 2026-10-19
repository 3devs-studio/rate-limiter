<?php

declare(strict_types=1);

namespace Fairshare;

final class UnknownServiceException extends FairshareException
{
    public function __construct(public readonly string $service)
    {
        parent::__construct("unknown service: $service");
    }
}
