"""Runs sdk/conformance/cases.json with both the sync and the asyncio client."""
from __future__ import annotations

import asyncio
import json
import os
import pathlib
import time

import pytest
import redis
import redis.asyncio as aredis

from fairshare import FairShare, Share, Spec, UnknownServiceError, publish
from fairshare.aio import AsyncFairShare
from fairshare.aio import publish as apublish

SUITE = json.loads((pathlib.Path(__file__).parent / "../../conformance/cases.json").resolve().read_text())
ADDR = os.environ.get("REDIS_ADDR", "localhost:6379")
HOST, PORT = ADDR.split(":")
TOL = SUITE.get("tolerance", 0.1)


def _available() -> bool:
    try:
        redis.Redis(host=HOST, port=int(PORT), socket_connect_timeout=1).ping()
        return True
    except redis.RedisError:
        return False


pytestmark = pytest.mark.skipif(not _available(), reason=f"no Redis at {ADDR}")


def to_spec(raw: dict) -> Spec:
    return Spec(rate=raw["rate"], burst=raw["burst"], services={n: Share(s["rate"], s["burst"]) for n, s in raw["services"].items()})


def check_acquire(d, expect, where):
    assert d.allowed == expect["allowed"], f"{where}: {d}"
    if "mode" in expect:
        assert d.mode == expect["mode"], where
    if "service_tokens" in expect:
        assert abs(d.service_tokens - expect["service_tokens"]) <= TOL, f"{where}: {d.service_tokens}"
    if "global_tokens" in expect:
        assert abs(d.global_tokens - expect["global_tokens"]) <= TOL, f"{where}: {d.global_tokens}"
    if "min_waited_ms" in expect:
        assert d.waited_ms >= expect["min_waited_ms"], where
    if "min_retry_after_ms" in expect:
        assert d.retry_after_ms >= expect["min_retry_after_ms"], where
    if "max_retry_after_ms" in expect:
        assert d.retry_after_ms <= expect["max_retry_after_ms"], where


@pytest.mark.parametrize("case", SUITE["cases"], ids=[c["name"] for c in SUITE["cases"]])
def test_sync(case):
    r = redis.Redis(host=HOST, port=int(PORT))
    upstream = f"conformance-py-{case['name']}"
    publish(r, upstream, to_spec(case["spec"]))
    lim = FairShare(r, upstream)
    lim.reset()
    for i, step in enumerate(case["steps"]):
        where = f"{case['name']} step {i}"
        op = step["op"]
        if op == "acquire":
            check_acquire(lim.acquire(step["service"], step.get("cost", 1), step.get("wait_ms", 0) / 1000), step["expect"], where)
        elif op == "drain":
            count, last = 0, ""
            while count < step.get("max", 1000):
                d = lim.try_acquire(step["service"], 1)
                last = d.mode
                if not d.allowed:
                    break
                count += 1
            assert count == step["expect_count"], where
            if "expect_last_mode" in step:
                assert last == step["expect_last_mode"], where
        elif op == "acquire_error":
            with pytest.raises(UnknownServiceError):
                lim.try_acquire(step["service"], 1)
        elif op == "cooldown":
            lim.report_rate_limited(step["ms"] / 1000)
        elif op == "sync":
            lim.sync_remaining(step["remaining"])
        elif op == "sleep":
            time.sleep(step["ms"] / 1000)
        elif op == "reset":
            lim.reset()
        else:
            raise AssertionError(f"unknown op {op}")


@pytest.mark.parametrize("case", SUITE["cases"], ids=[c["name"] for c in SUITE["cases"]])
def test_async(case):
    async def run():
        r = aredis.Redis(host=HOST, port=int(PORT))
        upstream = f"conformance-py-async-{case['name']}"
        await apublish(r, upstream, to_spec(case["spec"]))
        lim = AsyncFairShare(r, upstream)
        await lim.reset()
        for i, step in enumerate(case["steps"]):
            where = f"{case['name']} step {i}"
            op = step["op"]
            if op == "acquire":
                check_acquire(await lim.acquire(step["service"], step.get("cost", 1), step.get("wait_ms", 0) / 1000), step["expect"], where)
            elif op == "drain":
                count, last = 0, ""
                while count < step.get("max", 1000):
                    d = await lim.try_acquire(step["service"], 1)
                    last = d.mode
                    if not d.allowed:
                        break
                    count += 1
                assert count == step["expect_count"], where
                if "expect_last_mode" in step:
                    assert last == step["expect_last_mode"], where
            elif op == "acquire_error":
                with pytest.raises(UnknownServiceError):
                    await lim.try_acquire(step["service"], 1)
            elif op == "cooldown":
                await lim.report_rate_limited(step["ms"] / 1000)
            elif op == "sync":
                await lim.sync_remaining(step["remaining"])
            elif op == "sleep":
                await asyncio.sleep(step["ms"] / 1000)
            elif op == "reset":
                await lim.reset()
        await r.aclose()

    asyncio.run(run())


def test_asgi_middleware():
    from starlette.applications import Starlette
    from starlette.responses import PlainTextResponse
    from starlette.routing import Route
    from starlette.testclient import TestClient

    from fairshare.asgi import FairShareMiddleware, service_from_header

    r = redis.Redis(host=HOST, port=int(PORT))
    publish(r, "conformance-asgi", Spec(rate=1, burst=2, services={"web": Share(1, 2)}))
    FairShare(r, "conformance-asgi").reset()

    app = Starlette(routes=[Route("/api/x", lambda req: PlainTextResponse("ok"))])
    lim = AsyncFairShare(aredis.Redis(host=HOST, port=int(PORT)), "conformance-asgi")
    app.add_middleware(FairShareMiddleware, limiter=lim, service=service_from_header("x-service"))
    # The context manager keeps one event loop for every request, like a real
    # server does; the asyncio Redis connection is bound to that loop.
    with TestClient(app) as client:
        codes = [client.get("/api/x", headers={"x-service": "web"}).status_code for _ in range(3)]
        assert codes == [200, 200, 429]
        res = client.get("/api/x", headers={"x-service": "web"})
        assert res.headers["x-fair-share-mode"] == "global" and res.headers["retry-after"]
        assert client.get("/api/x").status_code == 200  # no service header: unmanaged
