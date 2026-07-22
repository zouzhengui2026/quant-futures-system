"""Immutable models for market-environment timing assessments.

Timing assessments describe whether the current environment is worth watching.
They deliberately do not contain direction, entry, position, or order information.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from math import isfinite

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.observation.models import MarketObservation


class TimingStatus(str, Enum):
    """Market-environment readiness categories, not trading instructions."""

    WAITING = "waiting"
    WATCHING = "watching"
    FAVORABLE = "favorable"
    UNFAVORABLE = "unfavorable"


# A descriptive alias for callers that prefer the model-oriented name.
TimingAssessmentStatus = TimingStatus


class RuleOutcome(str, Enum):
    """Result of evaluating one market-environment condition."""

    PASS = "pass"
    NEUTRAL = "neutral"
    FAIL = "fail"


@dataclass(frozen=True, slots=True)
class RuleEvaluation:
    """The transparent result of a single timing rule."""

    rule: str
    outcome: RuleOutcome
    reason: str

    def __post_init__(self) -> None:
        if not isinstance(self.rule, str) or not self.rule.strip():
            raise DomainValidationError("rule must be a non-empty string")
        if not isinstance(self.outcome, RuleOutcome):
            raise DomainValidationError("outcome must be a RuleOutcome")
        if not isinstance(self.reason, str) or not self.reason.strip():
            raise DomainValidationError("reason must be a non-empty string")

    @property
    def passed(self) -> bool:
        return self.outcome is RuleOutcome.PASS


@dataclass(frozen=True, slots=True)
class TimingAssessment:
    """A point-in-time assessment of whether market conditions are suitable.

    The assessment preserves each rule result so consumers can explain the
    environmental classification without treating it as an alpha or signal.
    """

    observation: MarketObservation
    status: TimingStatus
    evaluations: tuple[RuleEvaluation, ...]
    assessed_at: datetime
    confidence: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.observation, MarketObservation):
            raise DomainValidationError("observation must be a MarketObservation")
        if not isinstance(self.status, TimingStatus):
            raise DomainValidationError("status must be a TimingStatus")
        if not isinstance(self.evaluations, tuple) or not self.evaluations:
            raise DomainValidationError("evaluations must be a non-empty tuple")
        if not all(isinstance(evaluation, RuleEvaluation) for evaluation in self.evaluations):
            raise DomainValidationError("evaluations must contain RuleEvaluation instances")
        if not isinstance(self.assessed_at, datetime) or self.assessed_at.tzinfo is None:
            raise DomainValidationError("assessed_at must be a timezone-aware datetime")
        if (
            not isinstance(self.confidence, (int, float))
            or isinstance(self.confidence, bool)
            or not isfinite(self.confidence)
            or not 0.0 <= self.confidence <= 1.0
        ):
            raise DomainValidationError("confidence must be a finite number between 0.0 and 1.0")

    @property
    def state(self) -> TimingStatus:
        """Alias for status, retained for state-oriented consumers."""
        return self.status

    @property
    def reasons(self) -> tuple[str, ...]:
        """Human-readable explanations contributed by all evaluated rules."""
        return tuple(evaluation.reason for evaluation in self.evaluations)
