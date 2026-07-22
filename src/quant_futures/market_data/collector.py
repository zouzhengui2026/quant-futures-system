"""Collection orchestration for normalized market data."""

from dataclasses import dataclass, field
from datetime import datetime

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import MarketDataError
from quant_futures.core.logger import get_logger
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.market_data.repository import InMemoryMarketDataRepository
from quant_futures.market_data.source import MarketDataSource


@dataclass(slots=True)
class MarketDataCollector:
    """Persist and publish validated observations without making trading decisions."""

    event_bus: EventBus
    repository: InMemoryMarketDataRepository = field(default_factory=InMemoryMarketDataRepository)
    _watermarks: dict[tuple[str, str, MarketDataKind], datetime] = field(
        default_factory=dict, init=False
    )

    def collect(self, source: MarketDataSource) -> int:
        """Read all currently available source records and return accepted count."""
        return sum(1 for record in source.read() if self.ingest(record))

    def ingest(self, record: MarketDataRecord) -> bool:
        """Store and publish *record*, rejecting out-of-order records safely."""
        if not isinstance(record, MarketDataRecord):
            raise MarketDataError("ingest expects a MarketDataRecord")

        key = (record.source, record.symbol, record.kind)
        watermark = self._watermarks.get(key)
        if watermark is not None and record.timestamp < watermark:
            self.event_bus.publish(
                Event(EventType.MARKET_DATA_REJECTED, {"record": record, "reason": "out_of_order"})
            )
            get_logger(__name__).warning("Rejected out-of-order market data for %s", record.symbol)
            return False

        self.repository.save(record)
        self._watermarks[key] = record.timestamp
        self.event_bus.publish(Event(EventType.MARKET_DATA_RECEIVED, {"record": record}))
        return True
