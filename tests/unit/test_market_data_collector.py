from datetime import datetime, timedelta, timezone

import pytest

from quant_futures.core.events import EventBus, EventType
from quant_futures.core.exceptions import MarketDataError
from quant_futures.market_data import (
    InstrumentMetadata,
    MarketDataCollector,
    MarketDataKind,
    MarketDataRecord,
)


def record(*, timestamp: datetime) -> MarketDataRecord:
    return MarketDataRecord(
        kind=MarketDataKind.MARK_PRICE,
        symbol="BTCUSDT",
        source="replay",
        timestamp=timestamp,
        values={"price": 60_000.0},
    )


def test_collector_stores_and_publishes_a_validated_observation() -> None:
    bus = EventBus()
    received = []
    bus.subscribe(EventType.MARKET_DATA_RECEIVED, received.append)
    collector = MarketDataCollector(bus)
    item = record(timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert collector.ingest(item) is True
    assert received[0].payload["record"] == item
    assert collector.repository.get_latest(
        source="replay", symbol="BTCUSDT", kind=MarketDataKind.MARK_PRICE
    ) == item


def test_collector_rejects_out_of_order_observations_without_overwriting_latest() -> None:
    bus = EventBus()
    rejected = []
    bus.subscribe(EventType.MARKET_DATA_REJECTED, rejected.append)
    collector = MarketDataCollector(bus)
    current = record(timestamp=datetime(2026, 1, 2, tzinfo=timezone.utc))

    collector.ingest(current)
    assert collector.ingest(record(timestamp=current.timestamp - timedelta(seconds=1))) is False
    assert rejected[0].payload["reason"] == "out_of_order"
    assert collector.repository.get_latest(
        source="replay", symbol="BTCUSDT", kind=MarketDataKind.MARK_PRICE
    ) == current


def test_market_data_record_requires_timezone_aware_timestamp() -> None:
    with pytest.raises(MarketDataError, match="timezone-aware"):
        record(timestamp=datetime(2026, 1, 1))


def test_instrument_metadata_converts_to_a_market_data_record() -> None:
    metadata = InstrumentMetadata(
        symbol="BTCUSDT",
        source="replay",
        contract_size=0.001,
        price_tick=0.1,
        quantity_step=0.001,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert metadata.as_record().kind is MarketDataKind.INSTRUMENT_METADATA
