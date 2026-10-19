"""Pure helpers shared by the sync and asyncio clients."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field

CONFIG_SCHEMA = 1


class FairShareError(Exception):
    """Base class for this package's errors."""


class UnknownServiceError(FairShareError):
    def __init__(self, service: str) -> None:
        super().__init__(f"unknown service: {service}")
        self.service = service


class NoConfigError(FairShareError):
    def __init__(self, upstream: str) -> None:
        super().__init__(f"no published config for upstream: {upstream}")
        self.upstream = upstream


@dataclass(frozen=True)
class Share:
    rate: float
    burst: float
    weight: float | None = None


@dataclass(frozen=True)
class Spec:
    rate: float
    burst: float
    services: dict[str, Share] = field(default_factory=dict)


@dataclass(frozen=True)
class Decision:
    allowed: bool
    mode: str  # own, borrow, share, global, cooldown
    retry_after_ms: int
    waited_ms: int
    global_tokens: float
    service_tokens: float


def config_key(upstream: str) -> str:
    return f"rl:{{{upstream}}}:config"


def global_key(upstream: str) -> str:
    return f"rl:{{{upstream}}}:g"


def service_key(upstream: str, service: str) -> str:
    return f"rl:{{{upstream}}}:s:{service}"


def parse_spec(raw: bytes | str | None, upstream: str) -> Spec:
    if raw is None:
        raise NoConfigError(upstream)
    doc = json.loads(raw)
    if doc.get("schema") != CONFIG_SCHEMA:
        raise FairShareError(f"published config for {upstream} has schema {doc.get('schema')}, expected {CONFIG_SCHEMA}")
    services = {
        name: Share(rate=float(s["rate"]), burst=float(s["burst"]), weight=s.get("weight"))
        for name, s in doc["services"].items()
    }
    return Spec(rate=float(doc["rate"]), burst=float(doc["burst"]), services=services)


def publish_doc(upstream: str, spec: Spec, by: str) -> str:
    return json.dumps(
        {
            "schema": CONFIG_SCHEMA,
            "upstream": upstream,
            "rate": spec.rate,
            "burst": spec.burst,
            "services": {
                n: {"rate": s.rate, "burst": s.burst, **({"weight": s.weight} if s.weight is not None else {})}
                for n, s in spec.services.items()
            },
            "published_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "published_by": by,
        }
    )


def acquire_call(upstream: str, spec: Spec, service: str, cost: float) -> tuple[list[str], list[float]]:
    """KEYS and ARGV for acquire.lua: global, mine, then the others by name."""
    me = spec.services.get(service)
    if me is None:
        raise UnknownServiceError(service)
    keys = [global_key(upstream), service_key(upstream, service)]
    args: list[float] = [cost, spec.rate, spec.burst, me.rate, me.burst]
    for name in sorted(spec.services):
        if name == service:
            continue
        other = spec.services[name]
        keys.append(service_key(upstream, name))
        args.extend((other.rate, other.burst))
    return keys, args


def parse_decision(reply: list, waited_ms: int = 0) -> Decision:
    allowed, retry_ms, mode, g, s = reply
    if isinstance(mode, bytes):
        mode = mode.decode()
    return Decision(
        allowed=int(allowed) == 1,
        mode=mode,
        retry_after_ms=int(retry_ms),
        waited_ms=waited_ms,
        global_tokens=float(g),
        service_tokens=float(s),
    )


def shares_from_weights(rate: float, burst: float, weights: dict[str, float]) -> dict[str, Share]:
    """Split rate and burst by weight, the way the service does."""
    total = sum(weights.values())
    return {
        name: Share(rate=rate * w / total, burst=max(1.0, burst * w / total), weight=w)
        for name, w in weights.items()
    }


def pause_ms(retry_after_ms: int, remaining_ms: float) -> float:
    return min(max(retry_after_ms, 5), remaining_ms, 1000)
