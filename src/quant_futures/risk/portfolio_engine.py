"""Serialized, complete-input portfolio exposure and limit evaluation."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from math import fsum
from threading import RLock, local
from weakref import ReferenceType, WeakKeyDictionary, ref

from quant_futures.account.models import AccountSnapshot
from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError, PortfolioRiskError
from quant_futures.portfolio.models import PositionSide

from .portfolio_models import (
    PortfolioRiskLimits, PortfolioRiskOutcome, PortfolioRiskSnapshot,
    PositionExposure, build_portfolio_risk_breaches, _zero,
)

PORTFOLIO_RISK_UPDATED = EventType.PORTFOLIO_RISK_UPDATED
_ACTIVE: ContextVar[frozenset[int]] = ContextVar("_ACTIVE_PORTFOLIO_RISK", default=frozenset())
_THREAD_ACTIVE = local()


@dataclass(frozen=True, slots=True, eq=False)
class _Identity:
    value: object

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Identity) and self.value is other.value


def _fingerprint(value: object) -> object:
    """Capture primitives and strong identities independently of public equality."""
    if value is None or isinstance(value, (str, int, float, bool, bytes, Enum)):
        return value
    if isinstance(value, tuple):
        return (_Identity(value), tuple(_fingerprint(item) for item in value))
    if isinstance(value, Mapping):
        return (_Identity(value), tuple(sorted((k, _fingerprint(v)) for k, v in value.items())))
    if is_dataclass(value):
        return (_Identity(value), tuple((f.name, _fingerprint(getattr(value, f.name))) for f in fields(value)))
    return _Identity(value)


@dataclass(frozen=True, slots=True)
class _Commitment:
    snapshot: object
    fingerprint: object


@dataclass(frozen=True, slots=True)
class _Anchor:
    lock: object
    event_bus_ref: ReferenceType[EventBus]
    limits: PortfolioRiskLimits
    limits_values: tuple[tuple[str, object], ...]
    limits_fingerprint: object
    history: list[PortfolioRiskSnapshot]
    commitments: list[_Commitment]


_ANCHORS: WeakKeyDictionary[PortfolioRiskEngine, _Anchor] = WeakKeyDictionary()


def _anchor(engine: PortfolioRiskEngine) -> _Anchor:
    try:
        return _ANCHORS[engine]
    except KeyError as exc:
        raise PortfolioRiskError("portfolio risk authority is unavailable") from exc


@dataclass(slots=True, eq=False, weakref_slot=True)
class PortfolioRiskEngine:
    event_bus: EventBus
    limits: PortfolioRiskLimits
    _latest: PortfolioRiskSnapshot | None = field(init=False, repr=False)
    _history: list[PortfolioRiskSnapshot] = field(init=False, repr=False)
    _lock: RLock = field(init=False, repr=False)
    _transition_active: bool = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self.event_bus, EventBus):
            raise DomainValidationError("event_bus must be an EventBus")
        if not isinstance(self.limits, PortfolioRiskLimits):
            raise DomainValidationError("limits must be PortfolioRiskLimits")
        self.limits.validate()
        self._latest = None
        self._history = []
        self._lock = RLock()
        self._transition_active = False
        limits_values = tuple(
            (item.name, getattr(self.limits, item.name))
            for item in fields(PortfolioRiskLimits)
        )
        _ANCHORS[self] = _Anchor(
            self._lock, ref(self.event_bus), self.limits, limits_values,
            _fingerprint(self.limits), self._history, [],
        )

    def evaluate(self, account_snapshot: AccountSnapshot) -> PortfolioRiskSnapshot:
        with self._guard() as (event_bus, limits):
            self._validate_committed()
            if not isinstance(account_snapshot, AccountSnapshot):
                raise DomainValidationError("account_snapshot must be an AccountSnapshot")
            account_snapshot.validate()
            limits.validate()
            if _fingerprint(limits) != _anchor(self).limits_fingerprint:
                raise PortfolioRiskError("portfolio risk limits were mutated")
            exposures = []
            for valuation in account_snapshot.valuations:
                position = valuation.position
                signed = 0.0 if position.side is PositionSide.FLAT else _zero(
                    position.signed_quantity * valuation.mark_price)
                exposures.append(PositionExposure(valuation, signed, _zero(abs(signed))))
            exposure_tuple = tuple(exposures)
            long_notional = _zero(fsum(max(e.signed_notional, 0.0) for e in exposure_tuple))
            short_notional = _zero(fsum(max(-e.signed_notional, 0.0) for e in exposure_tuple))
            gross = _zero(fsum((long_notional, short_notional)))
            net = _zero(fsum((long_notional, -short_notional)))
            largest = max((e.position_notional for e in exposure_tuple), default=0.0)
            concentration = 0.0 if gross == 0 else largest / gross
            multiple = None if account_snapshot.equity <= 0 else gross / account_snapshot.equity
            previous_peak = max((item.account_snapshot.equity for item in _anchor(self).history),
                                default=account_snapshot.starting_equity)
            peak = max(previous_peak, account_snapshot.equity)
            drawdown = 0.0 if peak <= 0 else (peak - account_snapshot.equity) / peak
            breach_tuple = build_portfolio_risk_breaches(
                account_snapshot, exposure_tuple, gross, net, concentration, multiple, limits,
                drawdown)
            snapshot = PortfolioRiskSnapshot(
                account_snapshot, exposure_tuple, long_notional, short_notional, gross, net,
                largest, concentration, multiple, limits, breach_tuple,
                PortfolioRiskOutcome.BREACHED if breach_tuple else PortfolioRiskOutcome.HEALTHY,
                account_snapshot.valued_at, drawdown,
            )
            # Revalidate both inputs after calculation, then construct the independent
            # commitment before any externally visible state changes.
            account_snapshot.validate()
            limits.validate()
            snapshot.validate()
            commitment = _Commitment(snapshot, _fingerprint(snapshot))
            anchor = _anchor(self)
            if _fingerprint(limits) != anchor.limits_fingerprint:
                raise PortfolioRiskError("portfolio risk limits changed during evaluation")
            anchor.history.append(snapshot)
            anchor.commitments.append(commitment)
            self._latest = snapshot
            event_bus.publish(Event(PORTFOLIO_RISK_UPDATED, {
                "portfolio_risk_snapshot": snapshot,
                "account_snapshot": account_snapshot,
                "position_exposures": snapshot.position_exposures,
                "limits": snapshot.limits,
                "breaches": snapshot.breaches,
            }, occurred_at=snapshot.evaluated_at))
            return snapshot

    def latest(self) -> PortfolioRiskSnapshot:
        with _anchor(self).lock:
            self._validate_committed()
            if not _anchor(self).history:
                raise PortfolioRiskError("portfolio risk history is empty")
            return _anchor(self).history[-1]

    def history(self) -> tuple[PortfolioRiskSnapshot, ...]:
        with _anchor(self).lock:
            self._validate_committed()
            return tuple(_anchor(self).history)

    def _validate_committed(self) -> None:
        try:
            anchor = _anchor(self)
            if self._history is not anchor.history:
                raise PortfolioRiskError("portfolio risk history authority was replaced")
            if self.limits is not anchor.limits or _fingerprint(anchor.limits) != anchor.limits_fingerprint:
                raise PortfolioRiskError("portfolio risk limits authority was changed")
            if len(anchor.history) != len(anchor.commitments):
                raise PortfolioRiskError("portfolio risk history differs from authority")
            expected_latest = anchor.history[-1] if anchor.history else None
            if self._latest is not expected_latest:
                raise PortfolioRiskError("latest portfolio risk identity was replaced")
            for snapshot, commitment in zip(anchor.history, anchor.commitments):
                if snapshot is not commitment.snapshot:
                    raise PortfolioRiskError("portfolio risk history identity was replaced")
                snapshot.validate()
                if _fingerprint(snapshot) != commitment.fingerprint:
                    raise PortfolioRiskError("committed portfolio risk snapshot was mutated")
        except (PortfolioRiskError, DomainValidationError):
            raise
        except Exception as exc:
            raise PortfolioRiskError("committed portfolio risk state is corrupt") from exc

    @contextmanager
    def _guard(self) -> Iterator[tuple[EventBus, PortfolioRiskLimits]]:
        engine_id = id(self)
        anchor = _anchor(self)
        event_bus = anchor.event_bus_ref()
        if event_bus is None:
            raise PortfolioRiskError("portfolio risk event bus is unavailable")
        thread_active = getattr(_THREAD_ACTIVE, "active", frozenset())
        if engine_id in thread_active or engine_id in _ACTIVE.get():
            raise PortfolioRiskError("portfolio risk evaluation must not be re-entered")
        with anchor.lock:
            active = _ACTIVE.get()
            thread_active = getattr(_THREAD_ACTIVE, "active", frozenset())
            if engine_id in active or engine_id in thread_active:
                raise PortfolioRiskError("portfolio risk evaluation must not be re-entered")
            token = _ACTIVE.set(active | {engine_id})
            _THREAD_ACTIVE.active = thread_active | {engine_id}
            self._transition_active = True
            try:
                yield event_bus, anchor.limits
            finally:
                # Frozen dataclasses can still be attacked through object.__setattr__.
                # Restore each field from primitive values captured at construction,
                # rather than merely restoring the shared limits object's identity.
                for name, value in anchor.limits_values:
                    object.__setattr__(anchor.limits, name, value)
                self._transition_active = False
                self._lock = anchor.lock
                self.event_bus = event_bus
                self.limits = anchor.limits
                self._history = anchor.history
                _THREAD_ACTIVE.active = thread_active
                _ACTIVE.reset(token)
