"""ASGI middleware: works with FastAPI, Starlette, Quart and any ASGI app."""
from __future__ import annotations

import json
import math
from collections.abc import Awaitable, Callable

from .aio import AsyncFairShare

Scope = dict
Receive = Callable[[], Awaitable[dict]]
Send = Callable[[dict], Awaitable[None]]


def _header(scope: Scope, name: str) -> str | None:
    want = name.lower().encode()
    for k, v in scope.get("headers", []):
        if k.lower() == want:
            return v.decode()
    return None


def service_from_header(name: str = "x-service") -> Callable[[Scope], str | None]:
    return lambda scope: _header(scope, name)


class FairShareMiddleware:
    """Admit or refuse incoming requests on behalf of the service that sent them.

    app.add_middleware(FairShareMiddleware, limiter=limiter, service=service_from_header("x-app-id"), wait=4.0)
    """

    def __init__(
        self,
        app,
        limiter: AsyncFairShare,
        service: Callable[[Scope], str | None] = service_from_header(),
        wait: float = 0,
        cost: float = 1,
        fail_open: bool = True,
    ) -> None:
        self.app = app
        self.limiter = limiter
        self.service = service
        self.wait = wait
        self.cost = cost
        self.fail_open = fail_open

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        service = self.service(scope)
        if not service:
            return await self.app(scope, receive, send)
        try:
            d = await self.limiter.acquire(service, self.cost, self.wait)
        except Exception:
            if not self.fail_open:
                return await _respond(send, 503, {"error": "rate limiter unavailable"}, [(b"x-fair-share-mode", b"unavailable")])
            return await self.app(scope, receive, _add_headers(send, [(b"x-fair-share-mode", b"unavailable")]))
        extra = [
            (b"x-fair-share-service", service.encode()),
            (b"x-fair-share-mode", d.mode.encode()),
            (b"x-fair-share-wait-ms", str(d.waited_ms).encode()),
        ]
        if not d.allowed:
            extra.append((b"retry-after", str(max(1, math.ceil(d.retry_after_ms / 1000))).encode()))
            body = {"error": "Too Many Requests", "service": service, "mode": d.mode, "retryAfterMs": d.retry_after_ms}
            return await _respond(send, 429, body, extra)
        await self.app(scope, receive, _add_headers(send, extra))


def _add_headers(send: Send, extra: list[tuple[bytes, bytes]]) -> Send:
    async def wrapped(message: dict) -> None:
        if message["type"] == "http.response.start":
            message = {**message, "headers": list(message.get("headers", [])) + extra}
        await send(message)

    return wrapped


async def _respond(send: Send, status: int, body: dict, extra: list[tuple[bytes, bytes]]) -> None:
    payload = json.dumps(body).encode()
    await send({"type": "http.response.start", "status": status, "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(payload)).encode()), *extra]})
    await send({"type": "http.response.body", "body": payload})
