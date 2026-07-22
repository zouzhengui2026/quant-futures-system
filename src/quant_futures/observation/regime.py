"""Rule-based market regime classification."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .features import MarketFeatures
from .models import LiquidityRegime, TrendRegime, VolatilityRegime


class MarketRegime(str, Enum):
    """Legacy aggregate regime retained for observation-pipeline consumers."""

    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    RANGE = "range"
    PANIC = "panic"
    RECOVERY = "recovery"
    RISK_OFF = "risk_off"


@dataclass(frozen=True, slots=True)
class RegimeSnapshot:
    """Independent classifications for trend, volatility, and liquidity."""

    trend: TrendRegime
    volatility: VolatilityRegime
    liquidity: LiquidityRegime


@dataclass(frozen=True, slots=True)
class RegimeResult:
    regime: MarketRegime
    confidence: float


class RegimeDetector:
    """Classify conditions using explicit thresholds and no signal generation."""

    def __init__(
        self,
        *,
        trend_threshold: float = 0.01,
        high_volatility_threshold: float = 0.03,
        low_volatility_threshold: float = 0.005,
        thin_liquidity_threshold: float = 0.20,
        stressed_liquidity_threshold: float = 0.70,
    ) -> None:
        self.trend_threshold = trend_threshold
        self.high_volatility_threshold = high_volatility_threshold
        self.low_volatility_threshold = low_volatility_threshold
        self.thin_liquidity_threshold = thin_liquidity_threshold
        self.stressed_liquidity_threshold = stressed_liquidity_threshold

    def detect_regimes(self, features: MarketFeatures) -> RegimeSnapshot:
        """Detect independent trend, volatility, and liquidity regimes."""
        trend = (
            TrendRegime.UPTREND if features.price_return >= self.trend_threshold else
            TrendRegime.DOWNTREND if features.price_return <= -self.trend_threshold else TrendRegime.RANGE
        )
        volatility = (
            VolatilityRegime.HIGH if features.volatility_level >= self.high_volatility_threshold else
            VolatilityRegime.LOW if features.volatility_level <= self.low_volatility_threshold else
            VolatilityRegime.NORMAL
        )
        liquidity = (
            LiquidityRegime.STRESSED if features.liquidity_stress >= self.stressed_liquidity_threshold else
            LiquidityRegime.THIN if features.liquidity_stress >= self.thin_liquidity_threshold else
            LiquidityRegime.LIQUID
        )
        return RegimeSnapshot(trend=trend, volatility=volatility, liquidity=liquidity)

    def detect(self, features: MarketFeatures) -> RegimeResult:
        """Return the prior aggregate view for compatibility with Phase 1 APIs."""
        regimes = self.detect_regimes(features)
        if regimes.volatility is VolatilityRegime.HIGH and regimes.liquidity is LiquidityRegime.STRESSED:
            return RegimeResult(MarketRegime.PANIC, 0.8)
        if regimes.trend is TrendRegime.UPTREND:
            return RegimeResult(MarketRegime.TREND_UP, 0.7)
        if regimes.trend is TrendRegime.DOWNTREND:
            return RegimeResult(MarketRegime.TREND_DOWN, 0.7)
        return RegimeResult(MarketRegime.RANGE, 0.5)
