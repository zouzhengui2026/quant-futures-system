"""In-process event primitives for decoupled runtime communication.

The bus is deliberately synchronous and in-process for Phase 1.  Its small,
explicit API gives Observation, Alpha, Decision, Risk, and Execution modules a
common communication boundary without introducing an external broker.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from threading import RLock
from typing import Any, TypeAlias
from uuid import UUID, uuid4

from quant_futures.core.exceptions import EventBusError

EventHandler: TypeAlias = Callable[["Event"], None]


class EventType(str, Enum):
    """Canonical event names shared by runtime layers.

    String event names remain supported for compatibility and for future
    extension without requiring a core release.
    """

    MARKET_SNAPSHOT = "observation.market_snapshot"
    OBSERVATION_UPDATED = "observation.updated"
    ALPHA_GENERATED = "alpha.generated"
    DECISION_CREATED = "decision.created"
    RISK_UPDATED = "risk.updated"
    EXECUTION_UPDATED = "execution.updated"


EventName: TypeAlias = str | EventType


@dataclass(frozen=True, slots=True)
class Event:
    """An immutable message emitted by one runtime component."""

    event_type: EventName
    payload: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    event_id: UUID = field(default_factory=uuid4)

    def __post_init__(self) -> None:
        if not self.event_type.strip():
            raise EventBusError("event_type must not be empty")
        if self.occurred_at.tzinfo is None:
            raise EventBusError("occurred_at must be timezone-aware")


class EventBus:
    """Thread-safe synchronous publish/subscribe event bus.

    Subscriber failures are allowed to propagate to the publisher.  This makes
    failed in-process processing visible instead of silently dropping a runtime
    event.  Future durable or asynchronous transports can implement the same
    publish/subscribe boundary.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[EventHandler]] = defaultdict(list)
        self._lock = RLock()

    def subscribe(self, event_type: EventName, handler: EventHandler) -> Callable[[], None]:
        """Register *handler* for an event type and return an unsubscribe callback."""
        if not event_type.strip():
            raise EventBusError("event_type must not be empty")
        if not callable(handler):
            raise EventBusError("handler must be callable")

        with self._lock:
            if handler not in self._subscribers[event_type]:
                self._subscribers[event_type].append(handler)

        def unsubscribe() -> None:
            self.unsubscribe(event_type, handler)

        return unsubscribe

    def unsubscribe(self, event_type: EventName, handler: EventHandler) -> None:
        """Remove a previously registered handler, if it is still registered."""
        with self._lock:
            handlers = self._subscribers.get(event_type)
            if not handlers:
                return
            try:
                handlers.remove(handler)
            except ValueError:
                return
            if not handlers:
                del self._subscribers[event_type]

    def publish(self, event: Event) -> int:
        """Deliver *event* to current subscribers and return the delivery count."""
        if not isinstance(event, Event):
            raise EventBusError("publish expects an Event instance")

        with self._lock:
            handlers = tuple(self._subscribers.get(event.event_type, ()))
        for handler in handlers:
            handler(event)
        return len(handlers)
