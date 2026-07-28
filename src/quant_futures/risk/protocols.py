"""Structural contracts for risk-review policies."""

from typing import Protocol

from quant_futures.decision.models import DecisionIntent

from .models import RiskAssessment


class RiskPolicy(Protocol):
    name: str

    def assess(self, decision: DecisionIntent) -> RiskAssessment:
        """Review a decision without creating execution instructions."""
        ...
