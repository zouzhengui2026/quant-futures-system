from dataclasses import dataclass


@dataclass(frozen=True)
class MarketFeatures:
    """Market structure features used by observation engines."""

    trend_strength: float
    volatility_level: float
    funding_extreme: float
    open_interest_change: float
    liquidity_stress: float
    crowding_risk: float
