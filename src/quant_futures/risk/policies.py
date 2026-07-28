"""Deterministic baseline risk-review policy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from typing import Callable

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.decision.models import DecisionAction, DecisionIntent
from quant_futures.timing.models import TimingStatus

from .models import RiskAssessment, RiskOutcome


@dataclass(frozen=True, slots=True)
class ThresholdRiskPolicy:
    """Approve valid directional proposals that meet transparent thresholds."""

    minimum_strength: float = 0.5
    minimum_confidence: float = 0.5
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    name: str = "threshold_risk_baseline"

    def __post_init__(self) -> None:
        self._validate_configuration()

    def assess(self, decision: DecisionIntent) -> RiskAssessment:
        self._validate_configuration()
        if not isinstance(decision, DecisionIntent):
            raise TypeError("assess expects a DecisionIntent")
        decision.validate()

        if decision.action is DecisionAction.ABSTAIN:
            return self._result(
                decision,
                RiskOutcome.REJECTED,
                ("no directional proposal is available for risk review",),
            )
        if decision.alpha_candidate.timing_assessment.status is TimingStatus.UNFAVORABLE:
            return self._result(
                decision,
                RiskOutcome.REJECTED,
                ("timing safety condition is unfavorable",),
            )

        failures: list[str] = []
        if decision.strength < self.minimum_strength:
            failures.append(
                f"strength below threshold: actual={decision.strength}, required={self.minimum_strength}"
            )
        if decision.confidence < self.minimum_confidence:
            failures.append(
                f"confidence below threshold: actual={decision.confidence}, required={self.minimum_confidence}"
            )
        if failures:
            return self._result(decision, RiskOutcome.REJECTED, tuple(failures))
        return self._result(
            decision,
            RiskOutcome.APPROVED,
            (f"{decision.action.value} passed strength and confidence risk thresholds",),
        )

    def _result(
        self,
        decision: DecisionIntent,
        outcome: RiskOutcome,
        reasons: tuple[str, ...],
    ) -> RiskAssessment:
        return RiskAssessment(
            symbol=decision.symbol,
            source=decision.source,
            assessed_at=self.clock(),
            policy_name=self.name,
            outcome=outcome,
            reasons=reasons,
            decision_intent=decision,
        )

    def _validate_configuration(self) -> None:
        self._threshold("minimum_strength", self.minimum_strength)
        self._threshold("minimum_confidence", self.minimum_confidence)
        if not isinstance(self.name, str) or not self.name.strip():
            raise DomainValidationError("name must be a non-empty string")
        if not callable(self.clock):
            raise DomainValidationError("clock must be callable")

    @staticmethod
    def _threshold(name: str, value: object) -> None:
        if (
            not isinstance(value, Real)
            or isinstance(value, bool)
            or not isfinite(value)
            or not 0.0 <= value <= 1.0
        ):
            raise DomainValidationError(f"{name} must be a finite number between 0.0 and 1.0")
