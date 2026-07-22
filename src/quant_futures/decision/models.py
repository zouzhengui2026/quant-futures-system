"""Immutable decision proposals produced from validated alpha candidates."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from math import isfinite
from numbers import Real

from quant_futures.alpha.models import AlphaCandidate, AlphaDirection
from quant_futures.core.exceptions import DomainValidationError
from quant_futures.timing.models import TimingStatus


class DecisionAction(str, Enum):
    """A proposal for Risk to review, never an execution instruction."""

    PROPOSE_LONG = "propose_long"
    PROPOSE_SHORT = "propose_short"
    ABSTAIN = "abstain"


@dataclass(frozen=True, slots=True)
class DecisionIntent:
    """An explainable, non-executable proposal derived from one alpha candidate."""

    symbol: str
    source: str
    created_at: datetime
    policy_name: str
    action: DecisionAction
    strength: float
    confidence: float
    reasons: tuple[str, ...]
    alpha_candidate: AlphaCandidate

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Revalidate all invariants before an intent crosses a layer boundary."""
        self._validate_non_empty_string("symbol", self.symbol)
        self._validate_non_empty_string("source", self.source)
        self._validate_non_empty_string("policy_name", self.policy_name)
        if not isinstance(self.action, DecisionAction):
            raise DomainValidationError("action must be a DecisionAction")
        if not isinstance(self.alpha_candidate, AlphaCandidate):
            raise DomainValidationError("alpha_candidate must be an AlphaCandidate")
        if not isinstance(self.created_at, datetime) or self.created_at.tzinfo is None:
            raise DomainValidationError("created_at must be a timezone-aware datetime")
        if self.created_at < self.alpha_candidate.generated_at:
            raise DomainValidationError("created_at must not be earlier than alpha_candidate.generated_at")
        self._validate_score("strength", self.strength)
        self._validate_score("confidence", self.confidence)
        if not isinstance(self.reasons, tuple) or not self.reasons:
            raise DomainValidationError("reasons must be a non-empty tuple")
        if not all(isinstance(reason, str) and reason.strip() for reason in self.reasons):
            raise DomainValidationError("reasons must contain non-empty strings")
        if self.symbol != self.alpha_candidate.symbol or self.source != self.alpha_candidate.source:
            raise DomainValidationError("symbol and source must match alpha_candidate")
        if self.strength > self.alpha_candidate.strength:
            raise DomainValidationError("decision strength must not exceed alpha_candidate.strength")
        if self.confidence > self.alpha_candidate.confidence:
            raise DomainValidationError("decision confidence must not exceed alpha_candidate.confidence")

        direction = self.alpha_candidate.direction
        if self.alpha_candidate.timing_assessment.status is TimingStatus.UNFAVORABLE:
            if self.action is not DecisionAction.ABSTAIN:
                raise DomainValidationError("unfavorable timing may only produce ABSTAIN")
            if self.strength != 0.0 or self.confidence != 0.0:
                raise DomainValidationError("unfavorable timing ABSTAIN decisions must have zero strength and confidence")
        if direction is AlphaDirection.NEUTRAL and self.action is not DecisionAction.ABSTAIN:
            raise DomainValidationError("neutral alpha candidates may only produce ABSTAIN")
        if self.action is DecisionAction.ABSTAIN:
            if self.strength != 0.0 or self.confidence != 0.0:
                raise DomainValidationError("ABSTAIN decisions must have zero strength and confidence")
        elif self.action is DecisionAction.PROPOSE_LONG:
            if direction is not AlphaDirection.LONG:
                raise DomainValidationError("PROPOSE_LONG requires a LONG alpha candidate")
            if self.strength <= 0.0:
                raise DomainValidationError("PROPOSE_LONG requires strength greater than 0.0")
        elif self.action is DecisionAction.PROPOSE_SHORT:
            if direction is not AlphaDirection.SHORT:
                raise DomainValidationError("PROPOSE_SHORT requires a SHORT alpha candidate")
            if self.strength <= 0.0:
                raise DomainValidationError("PROPOSE_SHORT requires strength greater than 0.0")

    @staticmethod
    def _validate_non_empty_string(name: str, value: object) -> None:
        if not isinstance(value, str) or not value.strip():
            raise DomainValidationError(f"{name} must be a non-empty string")

    @staticmethod
    def _validate_score(name: str, value: object) -> None:
        if (
            not isinstance(value, Real)
            or isinstance(value, bool)
            or not isfinite(value)
            or not 0.0 <= value <= 1.0
        ):
            raise DomainValidationError(f"{name} must be a finite number between 0.0 and 1.0")
