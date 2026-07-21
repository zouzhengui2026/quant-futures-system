"""Health status primitives for services."""

from dataclasses import dataclass


@dataclass(frozen=True)
class HealthStatus:
    service: str
    healthy: bool
    message: str = ""
