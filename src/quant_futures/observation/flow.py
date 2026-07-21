from enum import Enum


class FlowState(str, Enum):
    """Capital and leverage flow states."""

    HEALTHY_EXPANSION = "healthy_expansion"
    LEVERAGED_EXPANSION = "leveraged_expansion"
    DISTRIBUTION = "distribution"
    LONG_SQUEEZE = "long_squeeze"
    SHORT_SQUEEZE = "short_squeeze"
    DELEVERAGING = "deleveraging"
    LIQUIDITY_VACUUM = "liquidity_vacuum"
