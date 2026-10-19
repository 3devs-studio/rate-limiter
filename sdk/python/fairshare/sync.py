"""Synchronous client (redis.Redis)."""
from __future__ import annotations

import time

import redis

from . import _scripts
from ._core import (
    Decision,
    NoConfigError,
    Spec,
    acquire_call,
    config_key,
    global_key,
    parse_decision,
    parse_spec,
    pause_ms,
    publish_doc,
    service_key,
)


class FairShare:
    """Admits requests for one upstream, straight from Redis."""

    def __init__(self, client: redis.Redis, upstream: str, config_ttl: float = 10.0) -> None:
        self.redis = client
        self.upstream = upstream
        self.config_ttl = config_ttl
        self._spec: Spec | None = None
        self._loaded = 0.0
        self._acquire = client.register_script(_scripts.ACQUIRE)
        self._cooldown = client.register_script(_scripts.COOLDOWN)
        self._sync = client.register_script(_scripts.SYNC)

    def spec(self) -> Spec:
        """The upstream's shares as published to Redis (cached)."""
        if self._spec is not None and time.monotonic() - self._loaded < self.config_ttl:
            return self._spec
        try:
            self._spec = parse_spec(self.redis.get(config_key(self.upstream)), self.upstream)
        except redis.RedisError:
            if self._spec is not None:
                return self._spec  # Redis hiccup: keep what we have
            raise
        self._loaded = time.monotonic()
        return self._spec

    def try_acquire(self, service: str, cost: float = 1) -> Decision:
        """Ask once; never waits."""
        keys, args = acquire_call(self.upstream, self.spec(), service, cost)
        return parse_decision(self._acquire(keys=keys, args=args))

    def acquire(self, service: str, cost: float = 1, wait: float = 0) -> Decision:
        """Keep asking until a token is granted or `wait` seconds have elapsed."""
        start = time.monotonic()
        d = self.try_acquire(service, cost)
        while not d.allowed:
            remaining_ms = wait * 1000 - (time.monotonic() - start) * 1000
            if remaining_ms <= 0:
                break
            time.sleep(pause_ms(d.retry_after_ms, remaining_ms) / 1000)
            d = self.try_acquire(service, cost)
        return Decision(**{**d.__dict__, "waited_ms": int((time.monotonic() - start) * 1000)})

    def report_rate_limited(self, retry_after: float) -> None:
        """The upstream answered 429: every service backs off for `retry_after` seconds."""
        spec = self.spec()
        self._cooldown(keys=[global_key(self.upstream)], args=[max(1, round(retry_after * 1000)), spec.rate, spec.burst])

    def sync_remaining(self, remaining: float) -> None:
        """Lower the global bucket to what the upstream says is left."""
        spec = self.spec()
        self._sync(keys=[global_key(self.upstream)], args=[remaining, spec.rate, spec.burst])

    def reset(self) -> None:
        """Refill every bucket."""
        spec = self.spec()
        self.redis.delete(global_key(self.upstream), *(service_key(self.upstream, s) for s in spec.services))


def publish(client: redis.Redis, upstream: str, spec: Spec, by: str = "fairshare-python") -> None:
    """Write an upstream's shares to Redis without running the service."""
    client.set(config_key(upstream), publish_doc(upstream, spec, by))


__all__ = ["FairShare", "publish", "NoConfigError"]
