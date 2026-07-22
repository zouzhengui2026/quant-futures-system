"""Feature extraction from normalized market-data records."""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from typing import Mapping

from quant_futures.market_data.models import MarketDataRecord


@dataclass(frozen=True, slots=True)
class MarketFeatures:
    """Numerical descriptors used only to describe market conditions."""

    trend_strength: float = 0.0
    volatility_level: float = 0.0
    funding_extreme: float = 0.0
    open_interest_change: float = 0.0
    liquidity_stress: float = 0.0
    crowding_risk: float = 0.0
    price_return: float = 0.0
    relative_volume: float = 1.0
    bid_ask_spread: float = 0.0


class FeatureBuilder:
    """Build bounded, deterministic features from each ``MarketDataRecord``.

    The builder keeps the prior price and volume per source/symbol, allowing it
    to calculate a simple return and relative volume without external services.
    Missing optional fields deliberately produce neutral values.
    """

    def __init__(self) -> None:
        self._previous_price: dict[tuple[str, str], float] = {}
        self._previous_volume: dict[tuple[str, str], float] = {}

    def build(self, record: MarketDataRecord) -> MarketFeatures:
        """Return features derived from *record* and its local history."""
        if not isinstance(record, MarketDataRecord):
            raise TypeError("build expects a MarketDataRecord")

        values = record.values
        price = self._number(values, "close", "price", "mark_price", "index_price")
        key = (record.source, record.symbol)
        previous_price = self._previous_price.get(key)
        price_return = (price - previous_price) / previous_price if price and previous_price else 0.0
        if price:
            self._previous_price[key] = price

        high, low = self._number(values, "high"), self._number(values, "low")
        volatility = abs(price_return)
        if price and high and low:
            volatility = max(volatility, abs(high - low) / price)

        volume = self._number(values, "volume", "quote_volume")
        prior_volume = self._previous_volume.get(key)
        relative_volume = volume / prior_volume if volume and prior_volume else 1.0
        if volume:
            self._previous_volume[key] = volume

        bid, ask = self._number(values, "bid"), self._number(values, "ask")
        spread = (ask - bid) / ((ask + bid) / 2) if bid > 0 and ask >= bid else 0.0
        liquidity_stress = max(spread, 1.0 / relative_volume if relative_volume > 0 else 1.0)

        funding = self._number(values, "funding_rate")
        oi_change = self._number(values, "open_interest_change", "oi_change")
        return MarketFeatures(
            trend_strength=self._bounded(price_return),
            volatility_level=self._bounded(volatility),
            funding_extreme=self._bounded(abs(funding) * 100),
            open_interest_change=self._bounded(oi_change),
            liquidity_stress=self._bounded(liquidity_stress),
            crowding_risk=self._bounded(max(abs(funding) * 100, abs(oi_change))),
            price_return=price_return,
            relative_volume=relative_volume,
            bid_ask_spread=spread,
        )

    @staticmethod
    def _number(values: Mapping[str, float | str | bool], *names: str) -> float:
        for name in names:
            value = values.get(name)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and isfinite(value):
                return float(value)
        return 0.0

    @staticmethod
    def _bounded(value: float) -> float:
        return max(-1.0, min(1.0, value))
