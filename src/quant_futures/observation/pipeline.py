from dataclasses import dataclass

from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.flow import FlowState
from quant_futures.observation.regime import MarketRegime
from quant_futures.observation.state import MarketObservation


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
            price=price,
            trend_state=regime.value,
            volatility_state=self._volatility_state(features),
            funding_state=self._funding_state(features),
            open_interest_state=self._oi_state(features),
            liquidity_state=self._liquidity_state(features),
            crowding_state=self._crowding_state(features, flow_state),
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
