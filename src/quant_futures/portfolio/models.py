"""Immutable audit records produced by portfolio accounting."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from math import fsum, isclose, isfinite
from numbers import Real

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.domain.order import OrderStatus
from quant_futures.execution.paper.models import PaperExecutionReport

ZERO_TOLERANCE = 1e-12
EMPTY_PORTFOLIO_TIMESTAMP = datetime(1970, 1, 1, tzinfo=timezone.utc)


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"
    FLAT = "flat"


def _finite(name: str, value: object) -> None:
    if not isinstance(value, Real) or isinstance(value, bool) or not isfinite(value):
        raise DomainValidationError(f"{name} must be a finite real number")


def _aware(name: str, value: object) -> None:
    if not isinstance(value, datetime):
        raise DomainValidationError(f"{name} must be a timezone-aware datetime")
    try:
        offset = value.utcoffset()
    except Exception as exc:
        raise DomainValidationError(f"{name} must be a timezone-aware datetime") from exc
    if value.tzinfo is None or offset is None:
        raise DomainValidationError(f"{name} must be a timezone-aware datetime")


def _normalize_zero(value: float) -> float:
    return 0.0 if abs(value) <= ZERO_TOLERANCE else value


def _realized_total(positions: tuple[PositionSnapshot, ...]) -> float:
    """Return the canonical stable portfolio aggregation."""
    return _normalize_zero(fsum(position.realized_pnl for position in positions))


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    source: str
    symbol: str
    signed_quantity: float
    side: PositionSide
    average_entry_price: float | None
    realized_pnl: float
    updated_at: datetime
    last_order_id: str

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        for name in ("source", "symbol", "last_order_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise DomainValidationError(f"{name} must be a non-empty string")
        _finite("signed_quantity", self.signed_quantity)
        _finite("realized_pnl", self.realized_pnl)
        _aware("updated_at", self.updated_at)
        if not isinstance(self.side, PositionSide):
            raise DomainValidationError("side must be a PositionSide")
        expected = (
            PositionSide.FLAT if self.signed_quantity == 0
            else PositionSide.LONG if self.signed_quantity > 0
            else PositionSide.SHORT
        )
        if self.side is not expected:
            raise DomainValidationError("side must match signed_quantity")
        if self.side is PositionSide.FLAT:
            if self.average_entry_price is not None:
                raise DomainValidationError("a flat position must not have an average entry price")
        else:
            _finite("average_entry_price", self.average_entry_price)
            if self.average_entry_price <= 0:
                raise DomainValidationError("average_entry_price must be positive")


@dataclass(frozen=True, slots=True)
class PortfolioSnapshot:
    positions: tuple[PositionSnapshot, ...]
    total_realized_pnl: float
    updated_at: datetime

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not isinstance(self.positions, tuple):
            raise DomainValidationError("positions must be a tuple")
        keys: list[tuple[str, str]] = []
        for position in self.positions:
            if not isinstance(position, PositionSnapshot):
                raise DomainValidationError("positions must contain PositionSnapshot values")
            position.validate()
            keys.append((position.source, position.symbol))
        if len(keys) != len(set(keys)):
            raise DomainValidationError("position keys must be unique")
        if keys != sorted(keys):
            raise DomainValidationError("positions must be sorted by source and symbol")
        _finite("total_realized_pnl", self.total_realized_pnl)
        expected_total = _realized_total(self.positions)
        if not isclose(self.total_realized_pnl, expected_total, rel_tol=0.0, abs_tol=ZERO_TOLERANCE):
            raise DomainValidationError("total_realized_pnl must equal position realized PnL")
        _aware("updated_at", self.updated_at)
        expected_time = (
            max(position.updated_at for position in self.positions)
            if self.positions else EMPTY_PORTFOLIO_TIMESTAMP
        )
        if self.updated_at != expected_time:
            raise DomainValidationError("updated_at must equal the latest position timestamp")
        if not self.positions and self.total_realized_pnl != 0.0:
            raise DomainValidationError("an empty portfolio must have zero realized PnL")


@dataclass(frozen=True, slots=True)
class PositionUpdate:
    execution_report: PaperExecutionReport
    previous_position: PositionSnapshot | None
    current_position: PositionSnapshot
    realized_pnl_delta: float
    portfolio_snapshot: PortfolioSnapshot
    applied_at: datetime

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        report = self.execution_report
        if not isinstance(report, PaperExecutionReport):
            raise DomainValidationError("execution_report must be a PaperExecutionReport")
        report.validate()
        if report.order.status is not OrderStatus.FILLED:
            raise DomainValidationError("execution_report order must be FILLED")
        if not isinstance(self.current_position, PositionSnapshot):
            raise DomainValidationError("current_position must be a PositionSnapshot")
        self.current_position.validate()
        intent = report.execution_intent
        current = self.current_position
        if (current.source, current.symbol) != (intent.source, intent.symbol):
            raise DomainValidationError("current position must match execution intent")
        if self.previous_position is not None:
            if not isinstance(self.previous_position, PositionSnapshot):
                raise DomainValidationError("previous_position must be a PositionSnapshot")
            self.previous_position.validate()
            if (self.previous_position.source, self.previous_position.symbol) != (current.source, current.symbol):
                raise DomainValidationError("previous position must have the current position key")
        _finite("realized_pnl_delta", self.realized_pnl_delta)
        expected, expected_delta = _account_position(self.previous_position, report)
        accounting_fields = (
            "source", "symbol", "signed_quantity", "side", "average_entry_price",
            "realized_pnl", "updated_at", "last_order_id",
        )
        if any(getattr(current, name) != getattr(expected, name) for name in accounting_fields):
            raise DomainValidationError("current position must equal the accounting transition")
        if self.realized_pnl_delta != expected_delta:
            raise DomainValidationError("realized_pnl_delta must equal the accounting transition")
        _aware("applied_at", self.applied_at)
        if self.applied_at != report.occurred_at or current.updated_at != self.applied_at:
            raise DomainValidationError("update timestamps must match the execution report")
        if current.last_order_id != report.order.order_id:
            raise DomainValidationError("current position must retain the report order ID")
        if not isinstance(self.portfolio_snapshot, PortfolioSnapshot):
            raise DomainValidationError("portfolio_snapshot must be a PortfolioSnapshot")
        self.portfolio_snapshot.validate()
        matches = [position for position in self.portfolio_snapshot.positions
                   if (position.source, position.symbol) == (current.source, current.symbol)]
        if len(matches) != 1 or matches[0] is not current:
            raise DomainValidationError("portfolio snapshot must contain the exact current position")


def _account_position(previous: PositionSnapshot | None,
                      report: PaperExecutionReport) -> tuple[PositionSnapshot, float]:
    """Apply one validated fill using average-cost accounting without ledger state."""
    if previous is not None:
        previous.validate()
    quantity = float(report.filled_quantity)
    price = float(report.average_fill_price)
    x = quantity if report.order.side.value == "buy" else -quantity
    q = previous.signed_quantity if previous else 0.0
    old_average = previous.average_entry_price if previous else None
    cumulative = previous.realized_pnl if previous else 0.0
    delta = 0.0
    if q == 0.0:
        new_quantity, new_average = x, price
    elif q * x > 0:
        new_quantity = q + x
        new_average = (abs(q) * old_average + abs(x) * price) / abs(new_quantity)
    else:
        closed = min(abs(q), abs(x))
        delta = ((price - old_average) if q > 0 else (old_average - price)) * closed
        new_quantity = q + x
        if abs(new_quantity) <= ZERO_TOLERANCE:
            new_quantity, new_average = 0.0, None
        elif q * new_quantity > 0:
            new_average = old_average
        else:
            new_average = price
    delta = _normalize_zero(delta)
    realized = _normalize_zero(fsum((cumulative, delta)))
    side = PositionSide.FLAT if new_quantity == 0.0 else (
        PositionSide.LONG if new_quantity > 0 else PositionSide.SHORT
    )
    intent = report.execution_intent
    return PositionSnapshot(
        intent.source, intent.symbol, new_quantity, side, new_average, realized,
        report.occurred_at, report.order.order_id,
    ), delta
