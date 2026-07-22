"""Immutable, directional market hypotheses produced by the alpha layer.

Alpha candidates express a view of an observed market only.  They intentionally
do not contain decision, risk, portfolio, or execution information.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from math import isfinite

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.observation.models import MarketObservation
from quant_futures.timing.models import TimingAssessment


class AlphaDirection(str, Enum):
    """The directional view expressed by an alpha hypothesis."""

    LONG = "long"
    SHORT = "short"
    NEUTRAL = "neutral"


@dataclass(frozen=True, slots=True)
class AlphaCandidate:
    """An explainable alpha hypothesis tied to one timing assessment."""

    symbol: str
    source: str
    observation_timestamp: datetime
    generated_at: datetime
    model_name: str
    direction: AlphaDirection
    strength: float
    confidence: float
    reasons: tuple[str, ...]
    observation: MarketObservation
    timing_assessment: TimingAssessment

    def __post_init__(self) -> None:
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise DomainValidationError("symbol must be a non-empty string")
        if not isinstance(self.source, str) or not self.source.strip():
            raise DomainValidationError("source must be a non-empty string")
        if not isinstance(self.model_name, str) or not self.model_name.strip():
            raise DomainValidationError("model_name must be a non-empty string")
        if not isinstance(self.direction, AlphaDirection):
            raise DomainValidationError("direction must be an AlphaDirection")
        self._validate_timestamp("observation_timestamp", self.observation_timestamp)
        self._validate_timestamp("generated_at", self.generated_at)
        self._validate_score("strength", self.strength)
        self._validate_score("confidence", self.confidence)
        if self.direction is AlphaDirection.NEUTRAL and self.strength != 0.0:
            raise DomainValidationError("neutral candidates must have strength equal to 0.0")
        if not isinstance(self.reasons, tuple) or not self.reasons:
            raise DomainValidationError("reasons must be a non-empty tuple")
        if not all(isinstance(reason, str) and reason.strip() for reason in self.reasons):
            raise DomainValidationError("reasons must contain non-empty strings")
        if not isinstance(self.observation, MarketObservation):
            raise DomainValidationError("observation must be a MarketObservation")
        if not isinstance(self.timing_assessment, TimingAssessment):
            raise DomainValidationError("timing_assessment must be a TimingAssessment")
        if self.observation != self.timing_assessment.observation:
            raise DomainValidationError("observation must match timing_assessment.observation")
        if self.symbol != self.observation.symbol or self.source != self.observation.source:
            raise DomainValidationError("symbol and source must match observation")
        if self.observation_timestamp != self.observation.timestamp:
            raise DomainValidationError("observation_timestamp must match observation.timestamp")

    @staticmethod
    def _validate_timestamp(name: str, value: object) -> None:
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise DomainValidationError(f"{name} must be a timezone-aware datetime")

    @staticmethod
    def _validate_score(name: str, value: object) -> None:
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isfinite(value)
            or not 0.0 <= value <= 1.0
        ):
            raise DomainValidationError(f"{name} must be a finite number between 0.0 and 1.0")
