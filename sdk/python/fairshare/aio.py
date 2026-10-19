"""asyncio client (redis.asyncio.Redis)."""
from __future__ import annotations

import asyncio
import time

import redis.asyncio as aredis
from redis import RedisError

from . import _scripts
from ._core import (
    Decision,
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


class AsyncFairShare:
    """Admits requests for one upstream, straight from Redis, without blocking the loop."""

    def __init__(self, client: aredis.Redis, upstream: str, config_ttl: float = 10.0) -> None:
        self.redis = client
        self.upstream = upstream
        self.config_ttl = config_ttl
        self._spec: Spec | None = None
        self._loaded = 0.0
        self._acquire = client.register_script(_scripts.ACQUIRE)
        self._cooldown = client.register_script(_scripts.COOLDOWN)
        self._sync = client.register_script(_scripts.SYNC)

    async def spec(self) -> Spec:
        if self._spec is not None and time.monotonic() - self._loaded < self.config_ttl:
            return self._spec
        try:
            self._spec = parse_spec(await self.redis.get(config_key(self.upstream)), self.upstream)
        except RedisError:
            if self._spec is not None:
                return self._spec
            raise
        self._loaded = time.monotonic()
        return self._spec

    async def try_acquire(self, service: str, cost: float = 1) -> Decision:
        keys, args = acquire_call(self.upstream, await self.spec(), service, cost)
        return parse_decision(await self._acquire(keys=keys, args=args))

    async def acquire(self, service: str, cost: float = 1, wait: float = 0) -> Decision:
        start = time.monotonic()
        d = await self.try_acquire(service, cost)
        while not d.allowed:
            remaining_ms = wait * 1000 - (time.monotonic() - start) * 1000
            if remaining_ms <= 0:
                break
            await asyncio.sleep(pause_ms(d.retry_after_ms, remaining_ms) / 1000)
            d = await self.try_acquire(service, cost)
        return Decision(**{**d.__dict__, "waited_ms": int((time.monotonic() - start) * 1000)})

    async def report_rate_limited(self, retry_after: float) -> None:
        spec = await self.spec()
        await self._cooldown(keys=[global_key(self.upstream)], args=[max(1, round(retry_after * 1000)), spec.rate, spec.burst])

    async def sync_remaining(self, remaining: float) -> None:
        spec = await self.spec()
        await self._sync(keys=[global_key(self.upstream)], args=[remaining, spec.rate, spec.burst])

    async def reset(self) -> None:
        spec = await self.spec()
        await self.redis.delete(global_key(self.upstream), *(service_key(self.upstream, s) for s in spec.services))


async def publish(client: aredis.Redis, upstream: str, spec: Spec, by: str = "fairshare-python") -> None:
    await client.set(config_key(upstream), publish_doc(upstream, spec, by))
