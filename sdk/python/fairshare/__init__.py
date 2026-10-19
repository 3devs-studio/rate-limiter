"""fairshare: fair-share rate limiting straight from Redis.

    from fairshare import FairShare
    limiter = FairShare(redis.Redis.from_url(REDIS_URL), "thirdparty")
    d = limiter.acquire("my-app", wait=4.0)
    if not d.allowed: ...  # back off for d.retry_after_ms

Async: `from fairshare.aio import AsyncFairShare`; ASGI middleware: `fairshare.asgi.FairShareMiddleware`.
"""
from ._core import (
    CONFIG_SCHEMA,
    Decision,
    FairShareError,
    NoConfigError,
    Share,
    Spec,
    UnknownServiceError,
    config_key,
    shares_from_weights,
)
from .sync import FairShare, publish

__all__ = [
    "CONFIG_SCHEMA",
    "Decision",
    "FairShare",
    "FairShareError",
    "NoConfigError",
    "Share",
    "Spec",
    "UnknownServiceError",
    "config_key",
    "publish",
    "shares_from_weights",
]
