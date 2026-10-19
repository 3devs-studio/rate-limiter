<?php

declare(strict_types=1);

namespace Fairshare;

final class NoConfigException extends FairshareException
{
    public function __construct(public readonly string $upstream)
    {
        parent::__construct("no published config for upstream: $upstream");
    }
}
