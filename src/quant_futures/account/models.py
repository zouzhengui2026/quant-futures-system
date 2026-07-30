"""Immutable mark-to-market account audit records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import fsum, isfinite
from numbers import Real

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio.models import (
    ZERO_TOLERANCE,
    PortfolioSnapshot,
    PositionSide,
    PositionSnapshot,
)


def _finite(name: str, value: object) -> None:
    if not isinstance(value, Real) or isinstance(value, bool) or not isfinite(value):
        raise DomainValidationError(f"{name} must be a finite real number")


def _normalize(value: float) -> float:
    return 0.0 if abs(value) <= ZERO_TOLERANCE else value


def _equal(actual: float, expected: float) -> bool:
    return actual == expected


@dataclass(frozen=True, slots=True)
class PositionValuation:
    position: PositionSnapshot
    mark_record: MarketDataRecord | None
    mark_price: float | None
    unrealized_pnl: float
    total_pnl: float
    valued_at: datetime

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.position, PositionSnapshot):
            raise DomainValidationError("position must be a PositionSnapshot")
        self.position.validate()
        _finite("unrealized_pnl", self.unrealized_pnl)
        _finite("total_pnl", self.total_pnl)
        if self.position.side is PositionSide.FLAT:
            if self.mark_record is not None or self.mark_price is not None:
                raise DomainValidationError("flat positions must not have a mark")
            if self.unrealized_pnl != 0.0:
                raise DomainValidationError("flat unrealized_pnl must be exactly zero")
            expected_time = self.position.updated_at
        else:
            record = self.mark_record
            if not isinstance(record, MarketDataRecord):
                raise DomainValidationError("active positions require a MarketDataRecord")
            try:
                record.validate()
            except Exception as exc:
                raise DomainValidationError("mark_record is invalid") from exc
            if record.kind is not MarketDataKind.MARK_PRICE:
                raise DomainValidationError("mark_record must be a mark price")
            if (record.source, record.symbol) != (self.position.source, self.position.symbol):
                raise DomainValidationError("mark_record must match the position key")
            if set(record.values) != {"price"}:
                raise DomainValidationError("mark values must contain exactly price")
            price = record.values["price"]
            _finite("mark price", price)
            if price <= 0:
                raise DomainValidationError("mark price must be positive")
            _finite("mark_price", self.mark_price)
            if self.mark_price != price:
                raise DomainValidationError("mark_price must equal the mark record price")
            if record.timestamp < self.position.updated_at:
                raise DomainValidationError("mark timestamp precedes the position")
            expected_unrealized = _normalize(
                (float(price) - self.position.average_entry_price)
                * self.position.signed_quantity
            )
            if not _equal(self.unrealized_pnl, expected_unrealized):
                raise DomainValidationError("unrealized_pnl does not match mark-to-market")
            expected_time = record.timestamp
        expected_total = _normalize(fsum((self.position.realized_pnl, self.unrealized_pnl)))
        if not _equal(self.total_pnl, expected_total):
            raise DomainValidationError("total_pnl does not match realized plus unrealized PnL")
        if self.valued_at != expected_time:
            raise DomainValidationError("valued_at does not match the valuation input")


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    portfolio_snapshot: PortfolioSnapshot
    valuations: tuple[PositionValuation, ...]
    starting_equity: float
    total_realized_pnl: float
    total_unrealized_pnl: float
    total_pnl: float
    equity: float
    valued_at: datetime
    # Product-level fees, funding, deposits, and withdrawals committed by the
    # account authority.  Kept last for backwards-compatible construction.
    cash_flow: float = 0.0

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.portfolio_snapshot, PortfolioSnapshot):
            raise DomainValidationError("portfolio_snapshot must be a PortfolioSnapshot")
        self.portfolio_snapshot.validate()
        if not isinstance(self.valuations, tuple):
            raise DomainValidationError("valuations must be a tuple")
        positions = self.portfolio_snapshot.positions
        if len(self.valuations) != len(positions):
            raise DomainValidationError("valuations must cover every position exactly once")
        keys: list[tuple[str, str]] = []
        for valuation, position in zip(self.valuations, positions):
            if not isinstance(valuation, PositionValuation):
                raise DomainValidationError("valuations must contain PositionValuation values")
            valuation.validate()
            if valuation.position is not position:
                raise DomainValidationError("valuation must retain exact portfolio position identity")
            keys.append((position.source, position.symbol))
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise DomainValidationError("valuations must have unique sorted position keys")
        _finite("starting_equity", self.starting_equity)
        if self.starting_equity < 0:
            raise DomainValidationError("starting_equity must be non-negative")
        for name in ("total_realized_pnl", "total_unrealized_pnl", "total_pnl", "equity", "cash_flow"):
            _finite(name, getattr(self, name))
        if not _equal(self.total_realized_pnl, self.portfolio_snapshot.total_realized_pnl):
            raise DomainValidationError("total_realized_pnl must equal the portfolio total")
        unrealized = _normalize(fsum(v.unrealized_pnl for v in self.valuations))
        total = _normalize(fsum((self.total_realized_pnl, unrealized)))
        equity = _normalize(fsum((self.starting_equity, total, self.cash_flow)))
        if not _equal(self.total_unrealized_pnl, unrealized):
            raise DomainValidationError("total_unrealized_pnl does not equal valuation total")
        if not _equal(self.total_pnl, total):
            raise DomainValidationError("total_pnl does not equal realized plus unrealized PnL")
        if not _equal(self.equity, equity):
            raise DomainValidationError("equity does not equal starting equity plus total PnL")
        expected_time = max((v.valued_at for v in self.valuations),
                            default=self.portfolio_snapshot.updated_at)
        if self.valued_at != expected_time:
            raise DomainValidationError("valued_at must equal the latest valuation timestamp")
