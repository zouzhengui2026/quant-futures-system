from datetime import datetime

from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.pipeline import ObservationPipeline
from quant_futures.observation.regime import MarketRegime
from quant_futures.observation.flow import FlowState


def test_observation_pipeline_builds_state():
    pipeline = ObservationPipeline()

    observation = pipeline.build(
        symbol="BTCUSDT",
        price=60000,
        features=MarketFeatures(
            trend_strength=0.8,
            volatility_level=0.3,
            funding_extreme=0.1,
            open_interest_change=0.2,
            liquidity_stress=0.1,
            crowding_risk=0.2,
        ),
        regime=MarketRegime.TREND_UP,
        flow_state=FlowState.HEALTHY_EXPANSION,
        timestamp=datetime.utcnow(),
    )

    assert observation.symbol == "BTCUSDT"
    assert observation.trend_state == MarketRegime.TREND_UP.value
