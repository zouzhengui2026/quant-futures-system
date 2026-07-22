from datetime import datetime, timezone

import pytest

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import EventBusError


def test_event_bus_delivers_events_to_subscribers() -> None:
    bus = EventBus()
    received: list[Event] = []
    event = Event("observation.market_snapshot", {"symbol": "BTCUSDT"})

    bus.subscribe("observation.market_snapshot", received.append)

    assert bus.publish(event) == 1
    assert received == [event]


def test_event_bus_accepts_centralized_event_type() -> None:
    bus = EventBus()
    received: list[Event] = []

    bus.subscribe(EventType.RISK_UPDATED, received.append)

    assert bus.publish(Event(EventType.RISK_UPDATED)) == 1
    assert received[0].event_type is EventType.RISK_UPDATED


def test_event_bus_unsubscribe_stops_delivery() -> None:
    bus = EventBus()
    received: list[Event] = []
    unsubscribe = bus.subscribe("risk.updated", received.append)

    unsubscribe()

    assert bus.publish(Event("risk.updated")) == 0
    assert received == []


def test_event_rejects_naive_timestamp() -> None:
    with pytest.raises(EventBusError, match="timezone-aware"):
        Event("decision.created", occurred_at=datetime(2026, 1, 1))


def test_event_accepts_aware_timestamp() -> None:
    event = Event("decision.created", occurred_at=datetime(2026, 1, 1, tzinfo=timezone.utc))

    assert event.event_type == "decision.created"
