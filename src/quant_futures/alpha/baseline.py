"""Deterministic baseline alpha model for validating the alpha architecture."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from typing import Callable

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.observation.models import TrendRegime
from quant_futures.timing.models import TimingAssessment, TimingStatus

from .models import AlphaCandidate, AlphaDirection


@dataclass(slots=True)
class RegimeDirectionalAlphaModel:
    """Express trend direction when timing safety conditions permit it.

    This deterministic model is an architecture baseline, not a prediction or
    production trading strategy.
    """

    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))
    name: str = "regime_directional_baseline"

    def generate(self, timing: TimingAssessment) -> AlphaCandidate:
        if not isinstance(timing, TimingAssessment):
            raise TypeError("generate expects a TimingAssessment")

        observation = timing.observation
        if timing.status is TimingStatus.UNFAVORABLE:
            direction = AlphaDirection.NEUTRAL
            strength = confidence = 0.0
            reasons = ("timing safety condition is unfavorable and blocks alpha generation",)
        elif observation.trend_regime is TrendRegime.UPTREND:
            direction = AlphaDirection.LONG
            strength = self._clamp(abs(self._trend_strength(observation.features.trend_strength)) * timing.confidence)
            confidence = self._clamp(timing.confidence)
            reasons = ("observed uptrend supports a long directional hypothesis",)
        elif observation.trend_regime is TrendRegime.DOWNTREND:
            direction = AlphaDirection.SHORT
            strength = self._clamp(abs(self._trend_strength(observation.features.trend_strength)) * timing.confidence)
            confidence = self._clamp(timing.confidence)
            reasons = ("observed downtrend supports a short directional hypothesis",)
        else:
            direction = AlphaDirection.NEUTRAL
            strength = 0.0
            confidence = self._clamp(timing.confidence)
            reasons = ("range regime provides no directional hypothesis in the baseline model",)

        return AlphaCandidate(
            symbol=observation.symbol,
            source=observation.source,
            observation_timestamp=observation.timestamp,
            generated_at=self.clock(),
            model_name=self.name,
            direction=direction,
            strength=strength,
            confidence=confidence,
            reasons=reasons,
            observation=observation,
            timing_assessment=timing,
        )

    @staticmethod
    def _clamp(value: float) -> float:
        if not isinstance(value, Real) or isinstance(value, bool) or not isfinite(value):
            raise DomainValidationError("alpha score must be a finite non-boolean real number")
        return max(0.0, min(1.0, value))

    @staticmethod
    def _trend_strength(value: object) -> float:
        """Return a validated feature value before deriving alpha strength."""
        if not isinstance(value, Real) or isinstance(value, bool) or not isfinite(value):
            raise DomainValidationError("trend_strength must be a finite non-boolean real number")
        return float(value)
