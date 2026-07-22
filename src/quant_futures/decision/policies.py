"""Deterministic baseline policies for the decision layer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from typing import Callable

from quant_futures.alpha.models import AlphaCandidate, AlphaDirection
from quant_futures.core.exceptions import DomainValidationError
from quant_futures.timing.models import TimingStatus

from .models import DecisionAction, DecisionIntent


@dataclass(slots=True)
class ThresholdDecisionPolicy:
    """Propose directional alpha only when timing and configured thresholds allow.

    This is a deterministic architecture baseline, not a production trading
    strategy or a prediction of future profitability.
    """

    minimum_strength: float = 0.5
    minimum_confidence: float = 0.5
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    name: str = "threshold_decision_baseline"

    def __post_init__(self) -> None:
        self._validate_threshold("minimum_strength", self.minimum_strength)
        self._validate_threshold("minimum_confidence", self.minimum_confidence)
        if not isinstance(self.name, str) or not self.name.strip():
            raise DomainValidationError("name must be a non-empty string")
        if not callable(self.clock):
            raise DomainValidationError("clock must be callable")

    def decide(self, candidate: AlphaCandidate) -> DecisionIntent:
        if not isinstance(candidate, AlphaCandidate):
            raise TypeError("decide expects an AlphaCandidate")
        if candidate.timing_assessment.status is TimingStatus.UNFAVORABLE:
            return self._abstain(candidate, "timing safety condition is unfavorable")
        if candidate.direction is AlphaDirection.NEUTRAL:
            return self._abstain(candidate, "neutral alpha candidate provides no directional proposal")
        failed = []
        if candidate.strength < self.minimum_strength:
            failed.append("strength")
        if candidate.confidence < self.minimum_confidence:
            failed.append("confidence")
        if failed:
            return self._abstain(
                candidate,
                "threshold not met for "
                f"{', '.join(failed)}: actual strength={candidate.strength}, "
                f"required strength={self.minimum_strength}, actual confidence={candidate.confidence}, "
                f"required confidence={self.minimum_confidence}",
            )
        action = DecisionAction.PROPOSE_LONG if candidate.direction is AlphaDirection.LONG else DecisionAction.PROPOSE_SHORT
        return DecisionIntent(
            symbol=candidate.symbol,
            source=candidate.source,
            created_at=self.clock(),
            policy_name=self.name,
            action=action,
            strength=candidate.strength,
            confidence=candidate.confidence,
            reasons=(f"directional alpha passed strength={self.minimum_strength} and confidence={self.minimum_confidence} thresholds",),
            alpha_candidate=candidate,
        )

    def _abstain(self, candidate: AlphaCandidate, reason: str) -> DecisionIntent:
        return DecisionIntent(
            symbol=candidate.symbol,
            source=candidate.source,
            created_at=self.clock(),
            policy_name=self.name,
            action=DecisionAction.ABSTAIN,
            strength=0.0,
            confidence=0.0,
            reasons=(reason,),
            alpha_candidate=candidate,
        )

    @staticmethod
    def _validate_threshold(name: str, value: object) -> None:
        if (
            not isinstance(value, Real)
            or isinstance(value, bool)
            or not isfinite(value)
            or not 0.0 <= value <= 1.0
        ):
            raise DomainValidationError(f"{name} must be a finite number between 0.0 and 1.0")
