"""Observation orchestration from market data to published market state."""

from __future__ import annotations

from dataclasses import dataclass, field

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.market_data.models import MarketDataRecord

from .features import FeatureBuilder
from .models import MarketObservation
from .regime import RegimeDetector

OBSERVATION_CREATED = EventType.OBSERVATION_CREATED


@dataclass(slots=True)
class ObservationEngine:
    """Build and publish observations; never produce orders or trading signals."""

    event_bus: EventBus
    feature_builder: FeatureBuilder = field(default_factory=FeatureBuilder)
    regime_detector: RegimeDetector = field(default_factory=RegimeDetector)

    def observe(self, record: MarketDataRecord) -> MarketObservation:
        """Convert one normalized record into and publish a market observation."""
        if not isinstance(record, MarketDataRecord):
            raise TypeError("observe expects a MarketDataRecord")
        features = self.feature_builder.build(record)
        regimes = self.regime_detector.detect_regimes(features)
        observation = MarketObservation(
            symbol=record.symbol,
            source=record.source,
            timestamp=record.timestamp,
            price=self._price(record),
            trend_regime=regimes.trend,
            volatility_regime=regimes.volatility,
            liquidity_regime=regimes.liquidity,
            features=features,
        )
        self.event_bus.publish(Event(OBSERVATION_CREATED, {"observation": observation, "record": record}))
        return observation

    @staticmethod
    def _price(record: MarketDataRecord) -> float:
        for key in ("close", "price", "mark_price", "index_price"):
            value = record.values.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
                return float(value)
        raise ValueError("MarketDataRecord needs a positive close, price, mark_price, or index_price")
