"""In-memory storage for the latest validated market observations."""

from quant_futures.market_data.models import MarketDataKind, MarketDataRecord


class InMemoryMarketDataRepository:
    """Small deterministic repository suitable for a local collection runtime.

    Storage is intentionally bounded to the latest record for each
    ``(source, symbol, kind)``.  Durable historical storage is a later
    infrastructure concern and is not silently simulated here.
    """

    def __init__(self) -> None:
        self._latest: dict[tuple[str, str, MarketDataKind], MarketDataRecord] = {}

    def save(self, record: MarketDataRecord) -> None:
        self._latest[(record.source, record.symbol, record.kind)] = record

    def get_latest(
        self, *, source: str, symbol: str, kind: MarketDataKind
    ) -> MarketDataRecord | None:
        return self._latest.get((source, symbol, kind))

    def records_for(self, *, source: str, symbol: str) -> tuple[MarketDataRecord, ...]:
        records = [
            record
            for (record_source, record_symbol, _), record in self._latest.items()
            if record_source == source and record_symbol == symbol
        ]
        return tuple(sorted(records, key=lambda record: record.kind.value))
