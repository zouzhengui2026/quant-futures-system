"""Exchange-agnostic input boundary for market data adapters."""

from collections.abc import Iterable
from typing import Protocol

from quant_futures.market_data.models import MarketDataRecord


class MarketDataSource(Protocol):
    """A read-only source of already-normalized market observations.

    Implementations may use REST, WebSockets, files, or replay data.  The
    interface intentionally has no trading or authentication-for-orders API.
    """

    def read(self) -> Iterable[MarketDataRecord]:
        """Return the observations currently available from this source."""
