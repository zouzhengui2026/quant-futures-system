from datetime import datetime, timezone

import pytest

from quant_futures.core.events import EventBus, EventType
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.observation import (
    FeatureBuilder,
    LiquidityRegime,
    ObservationEngine,
    RegimeDetector,
    TrendRegime,
    VolatilityRegime,
)


def record(values: dict[str, float]) -> MarketDataRecord:
    return MarketDataRecord(MarketDataKind.OHLCV, "BTCUSDT", "replay", datetime(2026, 1, 1, tzinfo=timezone.utc), values)


def test_feature_builder_uses_previous_price_and_ohlcv_range() -> None:
    builder = FeatureBuilder()
    builder.build(record({"close": 100.0, "volume": 10.0}))
    features = builder.build(record({"close": 102.0, "high": 104.0, "low": 100.0, "volume": 20.0}))

    assert features.price_return == pytest.approx(0.02)
    assert features.volatility_level == pytest.approx(4 / 102)
    assert features.relative_volume == 2.0


def test_regime_detector_classifies_all_three_dimensions() -> None:
    features = FeatureBuilder().build(record({"price": 100.0, "bid": 99.0, "ask": 100.0}))
    # Construct history to express a clear upward move.
    builder = FeatureBuilder()
    builder.build(record({"price": 100.0}))
    moved = builder.build(record({"price": 105.0, "high": 110.0, "low": 100.0, "bid": 90.0, "ask": 110.0}))
    regimes = RegimeDetector().detect_regimes(moved)

    assert regimes.trend is TrendRegime.UP
    assert regimes.volatility is VolatilityRegime.HIGH
    assert regimes.liquidity is LiquidityRegime.STRESSED
    assert features.liquidity_stress > 0


def test_engine_returns_observation_and_publishes_created_event() -> None:
    bus = EventBus()
    received = []
    bus.subscribe(EventType.OBSERVATION_CREATED, received.append)
    engine = ObservationEngine(bus)
    item = record({"close": 60_000.0, "high": 60_100.0, "low": 59_900.0, "volume": 100.0})

    observation = engine.observe(item)

    assert observation.symbol == "BTCUSDT"
    assert observation.price == 60_000.0
    assert observation.trend_regime is TrendRegime.RANGE
    assert received[0].payload["observation"] == observation
    assert received[0].payload["record"] == item


def test_engine_requires_a_price_field() -> None:
    with pytest.raises(ValueError, match="positive"):
        ObservationEngine(EventBus()).observe(record({"volume": 10.0}))
