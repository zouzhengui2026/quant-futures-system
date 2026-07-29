"""Validated, transport-neutral market data records.

These models deliberately contain observations only.  They do not expose order
or account concepts, which keeps the data layer safe to run independently of
trading capabilities.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from math import isfinite
from numbers import Real
from types import MappingProxyType
from typing import Mapping

from quant_futures.core.exceptions import MarketDataError


class MarketDataKind(str, Enum):
    """Market observations supported by the Phase 2 collection boundary."""

    OHLCV = "ohlcv"
    FUNDING_RATE = "funding_rate"
    OPEN_INTEREST = "open_interest"
    MARK_PRICE = "mark_price"
    INDEX_PRICE = "index_price"
    LIQUIDATION = "liquidation"
    INSTRUMENT_METADATA = "instrument_metadata"


@dataclass(frozen=True, slots=True)
class MarketDataRecord:
    """One normalized observation received from an exchange or historical feed."""

    kind: MarketDataKind
    symbol: str
    source: str
    timestamp: datetime
    values: Mapping[str, float | str | bool] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.values, Mapping):
            raise MarketDataError("values must be a mapping")
        object.__setattr__(self, "values", MappingProxyType(dict(self.values)))
        self.validate()

    def validate(self) -> None:
        """Revalidate the complete record, including after frozen-object tampering."""
        if not isinstance(self.kind, MarketDataKind):
            raise MarketDataError("kind must be a MarketDataKind")
        if not isinstance(self.symbol, str) or not self.symbol.strip():
            raise MarketDataError("symbol must not be empty")
        if not isinstance(self.source, str) or not self.source.strip():
            raise MarketDataError("source must not be empty")
        if not isinstance(self.timestamp, datetime):
            raise MarketDataError("timestamp must be timezone-aware")
        try:
            offset = self.timestamp.utcoffset()
        except Exception as exc:
            raise MarketDataError("timestamp must be timezone-aware") from exc
        if self.timestamp.tzinfo is None or offset is None:
            raise MarketDataError("timestamp must be timezone-aware")
        if not isinstance(self.values, Mapping) or not self.values:
            raise MarketDataError("values must not be empty")
        for name, value in self.values.items():
            if not isinstance(name, str) or not name.strip():
                raise MarketDataError("value names must not be empty")
            if isinstance(value, Real) and not isinstance(value, bool) and not isfinite(value):
                raise MarketDataError(f"{name} must be finite")


@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    """Validated static contract information supplied by an exchange."""

    symbol: str
    source: str
    contract_size: float
    price_tick: float
    quantity_step: float
    timestamp: datetime

    def __post_init__(self) -> None:
        if not self.symbol.strip() or not self.source.strip():
            raise MarketDataError("symbol and source must not be empty")
        if self.timestamp.tzinfo is None:
            raise MarketDataError("timestamp must be timezone-aware")
        for name, value in (
            ("contract_size", self.contract_size),
            ("price_tick", self.price_tick),
            ("quantity_step", self.quantity_step),
        ):
            if not isfinite(value) or value <= 0:
                raise MarketDataError(f"{name} must be a finite positive number")

    def as_record(self) -> MarketDataRecord:
        return MarketDataRecord(
            kind=MarketDataKind.INSTRUMENT_METADATA,
            symbol=self.symbol,
            source=self.source,
            timestamp=self.timestamp,
            values={
                "contract_size": self.contract_size,
                "price_tick": self.price_tick,
                "quantity_step": self.quantity_step,
            },
        )
