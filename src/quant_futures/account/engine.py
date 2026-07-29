"""Thread-safe complete-input simulated account equity engine."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator
from dataclasses import dataclass, field
from math import fsum, isfinite
from numbers import Real
from threading import RLock, local
from weakref import ReferenceType, WeakKeyDictionary, ref

from quant_futures.account.models import AccountSnapshot, PositionValuation, _normalize
from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import AccountValuationError, DomainValidationError
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio.models import PortfolioSnapshot, PositionSide

_ACTIVE: ContextVar[frozenset[int]] = ContextVar("_ACTIVE_ACCOUNT_ENGINES", default=frozenset())
_THREAD_ACTIVE = local()


@dataclass(frozen=True, slots=True)
class _Anchor:
    lock: object
    event_bus_ref: ReferenceType[EventBus]
    history: list[AccountSnapshot]
    committed_history: list[AccountSnapshot]
    starting_equity: float


_ANCHORS: WeakKeyDictionary[AccountEquityEngine, _Anchor] = WeakKeyDictionary()


def _anchor(engine: AccountEquityEngine) -> _Anchor:
    try:
        return _ANCHORS[engine]
    except KeyError as exc:
        raise AccountValuationError("account engine authority is unavailable") from exc


@dataclass(slots=True, eq=False, weakref_slot=True)
class AccountEquityEngine:
    event_bus: EventBus
    starting_equity: float
    _latest: AccountSnapshot | None = field(init=False, repr=False)
    _history: list[AccountSnapshot] = field(init=False, repr=False)
    _lock: RLock = field(init=False, repr=False)
    _transition_active: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.event_bus, EventBus):
            raise DomainValidationError("event_bus must be an EventBus")
        if (not isinstance(self.starting_equity, Real) or isinstance(self.starting_equity, bool)
                or not isfinite(self.starting_equity) or self.starting_equity < 0):
            raise DomainValidationError("starting_equity must be a finite non-negative real number")
        self._latest = None
        self._history = []
        self._lock = RLock()
        self._transition_active = False
        _ANCHORS[self] = _Anchor(
            self._lock, ref(self.event_bus), self._history, [], self.starting_equity)

    def value(self, portfolio_snapshot: PortfolioSnapshot,
              mark_records: tuple[MarketDataRecord, ...]) -> AccountSnapshot:
        with self._guard() as event_bus:
            self._validate_committed()
            if not isinstance(portfolio_snapshot, PortfolioSnapshot):
                raise DomainValidationError("portfolio_snapshot must be a PortfolioSnapshot")
            portfolio_snapshot.validate()
            if not isinstance(mark_records, tuple):
                raise AccountValuationError("mark_records must be a tuple")
            marks: dict[tuple[str, str], MarketDataRecord] = {}
            try:
                for record in mark_records:
                    if not isinstance(record, MarketDataRecord):
                        raise AccountValuationError("marks must contain MarketDataRecord values")
                    record.validate()
                    if record.kind is not MarketDataKind.MARK_PRICE:
                        raise AccountValuationError("every record must be a mark price")
                    key = (record.source, record.symbol)
                    if key in marks:
                        raise AccountValuationError(f"duplicate mark: {key[0]}/{key[1]}")
                    marks[key] = record
            except AccountValuationError:
                raise
            except Exception as exc:
                raise AccountValuationError("invalid mark record") from exc
            required = {(p.source, p.symbol) for p in portfolio_snapshot.positions
                        if p.side is not PositionSide.FLAT}
            if set(marks) != required:
                raise AccountValuationError("marks must exactly cover all non-flat positions")
            valuations = []
            for position in portfolio_snapshot.positions:
                record = marks.get((position.source, position.symbol))
                if record is None:
                    valuation = PositionValuation(position, None, None, 0.0,
                                                  _normalize(position.realized_pnl),
                                                  position.updated_at)
                else:
                    price = record.values.get("price")
                    try:
                        unrealized = _normalize(
                            (float(price) - position.average_entry_price)
                            * position.signed_quantity)
                    except Exception as exc:
                        raise AccountValuationError("invalid mark price") from exc
                    total = _normalize(fsum((position.realized_pnl, unrealized)))
                    valuation = PositionValuation(position, record, price, unrealized,
                                                  total, record.timestamp)
                valuations.append(valuation)
            valuation_tuple = tuple(valuations)
            unrealized_total = _normalize(fsum(v.unrealized_pnl for v in valuation_tuple))
            total = _normalize(fsum((portfolio_snapshot.total_realized_pnl, unrealized_total)))
            configured_equity = _anchor(self).starting_equity
            equity = _normalize(fsum((configured_equity, total)))
            valued_at = max((v.valued_at for v in valuation_tuple),
                            default=portfolio_snapshot.updated_at)
            snapshot = AccountSnapshot(
                portfolio_snapshot, valuation_tuple, configured_equity,
                portfolio_snapshot.total_realized_pnl, unrealized_total, total,
                equity, valued_at,
            )
            history = _anchor(self).history
            if history and snapshot.valued_at < history[-1].valued_at:
                raise AccountValuationError("valuation precedes the latest committed snapshot")
            history.append(snapshot)
            _anchor(self).committed_history.append(snapshot)
            self._latest = snapshot
            event_bus.publish(Event(EventType.ACCOUNT_UPDATED, {
                "account_snapshot": snapshot,
                "portfolio_snapshot": portfolio_snapshot,
                "valuations": snapshot.valuations,
                "mark_records": mark_records,
            }, occurred_at=snapshot.valued_at))
            return snapshot

    def latest(self) -> AccountSnapshot:
        with _anchor(self).lock:
            self._validate_committed()
            if not _anchor(self).history:
                raise AccountValuationError("account valuation history is empty")
            return _anchor(self).history[-1]

    def history(self) -> tuple[AccountSnapshot, ...]:
        with _anchor(self).lock:
            self._validate_committed()
            return tuple(_anchor(self).history)

    def _validate_committed(self) -> None:
        try:
            anchor = _anchor(self)
            if self._history is not anchor.history:
                raise AccountValuationError("account history authority was replaced")
            if (len(anchor.history) != len(anchor.committed_history)
                    or any(working is not committed for working, committed in zip(
                        anchor.history, anchor.committed_history))):
                raise AccountValuationError("account history differs from authority")
            expected_latest = anchor.committed_history[-1] if anchor.committed_history else None
            if self._latest is not expected_latest:
                raise AccountValuationError("latest account snapshot identity was replaced")
            for snapshot in anchor.committed_history:
                if not isinstance(snapshot, AccountSnapshot):
                    raise AccountValuationError("account history is structurally corrupt")
                snapshot.validate()
        except (AccountValuationError, DomainValidationError):
            raise
        except Exception as exc:
            raise AccountValuationError("committed account state is structurally corrupt") from exc

    @contextmanager
    def _guard(self) -> Iterator[EventBus]:
        engine_id = id(self)
        anchor = _anchor(self)
        event_bus = anchor.event_bus_ref()
        if event_bus is None:
            raise AccountValuationError("account engine event bus is unavailable")
        thread_active = getattr(_THREAD_ACTIVE, "active", frozenset())
        if engine_id in thread_active or engine_id in _ACTIVE.get():
            raise AccountValuationError("account valuations must not be re-entered")
        with anchor.lock:
            active = _ACTIVE.get()
            thread_active = getattr(_THREAD_ACTIVE, "active", frozenset())
            if engine_id in active or engine_id in thread_active:
                raise AccountValuationError("account valuations must not be re-entered")
            token = _ACTIVE.set(active | {engine_id})
            _THREAD_ACTIVE.active = thread_active | {engine_id}
            self._transition_active = True
            try:
                yield event_bus
            finally:
                self._transition_active = False
                self._lock = anchor.lock
                self.event_bus = event_bus
                self.starting_equity = anchor.starting_equity
                self._history = anchor.history
                _THREAD_ACTIVE.active = thread_active
                _ACTIVE.reset(token)
