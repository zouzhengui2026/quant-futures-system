from dataclasses import dataclass

from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.flow import FlowState
from quant_futures.observation.regime import MarketRegime
from quant_futures.observation.models import (
    LiquidityRegime, MarketObservation, TrendRegime, VolatilityRegime,
)


@dataclass
class ObservationPipeline:
    """Aggregate market features into a market observation.

    This layer only describes market state. It must never create
    trading signals or execute orders.
    """

    def build(
        self,
        symbol: str,
        price: float,
        features: MarketFeatures,
        regime: MarketRegime,
        flow_state: FlowState,
        timestamp,
    ) -> MarketObservation:
        return MarketObservation(
            symbol=symbol,
            source="derived",
            price=price,
            trend_regime={
                MarketRegime.TREND_UP: TrendRegime.UP,
                MarketRegime.TREND_DOWN: TrendRegime.DOWN,
            }.get(regime, TrendRegime.RANGE),
            volatility_regime=(
                VolatilityRegime.HIGH if features.volatility_level > 0.7 else VolatilityRegime.NORMAL
            ),
            liquidity_regime=(
                LiquidityRegime.STRESSED if features.liquidity_stress > 0.7 else LiquidityRegime.LIQUID
            ),
            features=features,
            timestamp=timestamp,
        )

    def _volatility_state(self, features: MarketFeatures) -> str:
        return "elevated" if features.volatility_level > 0.7 else "normal"

    def _funding_state(self, features: MarketFeatures) -> str:
        return "extreme" if abs(features.funding_extreme) > 0.8 else "normal"

    def _oi_state(self, features: MarketFeatures) -> str:
        return "expanding" if features.open_interest_change > 0 else "contracting"

    def _liquidity_state(self, features: MarketFeatures) -> str:
        return "stressed" if features.liquidity_stress > 0.7 else "healthy"

    def _crowding_state(self, features: MarketFeatures, flow_state: FlowState) -> str:
        if features.crowding_risk > 0.8:
            return "crowded"
        return flow_state.value
