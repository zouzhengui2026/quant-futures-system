"""Structural policy boundary for execution-intent creation."""

from typing import Protocol

from quant_futures.risk.models import RiskAssessment

from .models import ExecutionIntent


class ExecutionPolicy(Protocol):
    name: str

    def create_intent(self, risk_assessment: RiskAssessment) -> ExecutionIntent:
        """Create, but never submit, a validated intent."""
        ...
