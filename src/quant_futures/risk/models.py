"""Immutable, non-executable results of risk review."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.decision.models import DecisionAction, DecisionIntent
from quant_futures.timing.models import TimingStatus


class RiskOutcome(str, Enum):
    """The approval result of reviewing a decision proposal."""

    APPROVED = "approved"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    """A validated audit record; it is deliberately not an order instruction."""

    symbol: str
    source: str
    assessed_at: datetime
    policy_name: str
    outcome: RiskOutcome
    reasons: tuple[str, ...]
    decision_intent: DecisionIntent

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Revalidate every invariant whenever the record crosses a boundary."""
        self._non_empty("symbol", self.symbol)
        self._non_empty("source", self.source)
        self._non_empty("policy_name", self.policy_name)
        if not isinstance(self.decision_intent, DecisionIntent):
            raise DomainValidationError("decision_intent must be a DecisionIntent")
        self.decision_intent.validate()
        if not isinstance(self.assessed_at, datetime):
            raise DomainValidationError("assessed_at must be a timezone-aware datetime")
        try:
            offset = self.assessed_at.utcoffset()
        except Exception as exc:
            raise DomainValidationError("assessed_at must be a timezone-aware datetime") from exc
        if self.assessed_at.tzinfo is None or offset is None:
            raise DomainValidationError("assessed_at must be a timezone-aware datetime")
        if self.assessed_at < self.decision_intent.created_at:
            raise DomainValidationError("assessed_at must not be earlier than decision_intent.created_at")
        if not isinstance(self.outcome, RiskOutcome):
            raise DomainValidationError("outcome must be a RiskOutcome")
        if not isinstance(self.reasons, tuple) or not self.reasons:
            raise DomainValidationError("reasons must be a non-empty tuple")
        if not all(isinstance(reason, str) and reason.strip() for reason in self.reasons):
            raise DomainValidationError("reasons must contain non-empty strings")
        if self.symbol != self.decision_intent.symbol or self.source != self.decision_intent.source:
            raise DomainValidationError("symbol and source must match decision_intent")
        if self.outcome is RiskOutcome.APPROVED:
            if self.decision_intent.action not in (
                DecisionAction.PROPOSE_LONG,
                DecisionAction.PROPOSE_SHORT,
            ):
                raise DomainValidationError("only a directional proposal may be approved")
            if self.decision_intent.alpha_candidate.timing_assessment.status is TimingStatus.UNFAVORABLE:
                raise DomainValidationError("unfavorable timing may not be approved")

    @staticmethod
    def _non_empty(name: str, value: object) -> None:
        if not isinstance(value, str) or not value.strip():
            raise DomainValidationError(f"{name} must be a non-empty string")
