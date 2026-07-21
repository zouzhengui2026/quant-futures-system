from dataclasses import dataclass
from enum import Enum

from .features import MarketFeatures


class MarketRegime(str, Enum):
    """High-level market regimes."""

    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    RANGE = "range"
    PANIC = "panic"
    RECOVERY = "recovery"
    RISK_OFF = "risk_off"


@dataclass(frozen=True)
class RegimeResult:
    regime: MarketRegime
    confidence: float


class RegimeDetector:
    """Interpret market conditions only.

    This component does not create trading signals.
    It only describes the current environment.
    """

    def detect(self, features: MarketFeatures) -> RegimeResult:
        if features.volatility_level > 0.8 and features.liquidity_stress > 0.7:
            return RegimeResult(MarketRegime.PANIC, 0.8)

        if features.trend_strength > 0.6:
            return RegimeResult(MarketRegime.TREND_UP, 0.7)

        if features.trend_strength < -0.6:
            return RegimeResult(MarketRegime.TREND_DOWN, 0.7)

        return RegimeResult(MarketRegime.RANGE, 0.5)
