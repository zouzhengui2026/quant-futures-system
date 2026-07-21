from dataclasses import dataclass
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


@dataclass(frozen=True)
class FlowResult:
    state: FlowState
    confidence: float


class FlowAnalyzer:
    """Analyze market energy flow, not trade direction."""

    def analyze(self, funding_extreme: float, oi_change: float, liquidity_stress: float) -> FlowResult:
        if liquidity_stress > 0.8:
            return FlowResult(FlowState.LIQUIDITY_VACUUM, 0.8)

        if oi_change > 0.7 and abs(funding_extreme) > 0.7:
            return FlowResult(FlowState.LEVERAGED_EXPANSION, 0.75)

        if oi_change < -0.6:
            return FlowResult(FlowState.DELEVERAGING, 0.7)

        return FlowResult(FlowState.HEALTHY_EXPANSION, 0.5)
