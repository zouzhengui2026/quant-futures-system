"""Risk domain primitives."""

from dataclasses import dataclass


@dataclass(frozen=True)
class RiskCheckResult:
    allowed: bool
    reason: str
