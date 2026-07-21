from enum import Enum


class MarketRegime(str, Enum):
    """High-level market regimes."""

    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    RANGE = "range"
    PANIC = "panic"
    RECOVERY = "recovery"
    RISK_OFF = "risk_off"
