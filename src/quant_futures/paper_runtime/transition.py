"""Incremental, authoritative Paper market-event transitions.

This module is deliberately a domain coordinator: it owns neither filesystem
discovery nor a CLI/runtime loop.  Accounting and risk results are retained
from the Product v0.1 authorities rather than recalculated here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from math import isfinite
from typing import Callable

from quant_futures.account import AccountEquityEngine
from quant_futures.alpha import AlphaDirection
from quant_futures.core.events import EventBus
from quant_futures.decision import DecisionEngine
from quant_futures.decision.policies import ThresholdDecisionPolicy
from quant_futures.execution import ExecutionEngine, FixedQuantityExecutionPolicy, PaperExecutionEngine
from quant_futures.market_data.models import MarketDataKind, MarketDataRecord
from quant_futures.portfolio import PortfolioLedger, PositionSide
from quant_futures.portfolio.models import PortfolioSnapshot, PositionSnapshot
from quant_futures.product.config import ProductConfig
from quant_futures.product.data import Bar
from quant_futures.product.engine import _candidate, _position
from quant_futures.product.strategy import Strategy, StrategyContext
from quant_futures.risk import RiskEngine
from quant_futures.risk.policies import ThresholdRiskPolicy
from quant_futures.risk.portfolio_engine import PortfolioRiskEngine
from quant_futures.risk.portfolio_models import PortfolioRiskLimits

from .journal import JournalSnapshot, TransitionJournal
from .checkpoint import CheckpointError, CheckpointStore
from .lifecycle import Lifecycle, LifecycleError, LifecycleState
from .lock import RunDirectoryLock

VALID_STAGES = frozenset({
    "transition_started", "input_committed", "strategy_committed",
    "order_submitted", "fill_prepared", "fill_committed",
    "portfolio_committed", "account_committed", "risk_committed",
    "transition_committed",
})
class TransitionError(RuntimeError):
    """The incremental transition is inconsistent and must fail closed."""


class StageProtocol:
    """Validate a single input's ordered, unique stage sequence."""

    def __init__(self) -> None:
        self._seen: set[str] = set()

    def accept(self, stage: str) -> None:
        if stage not in VALID_STAGES or stage in self._seen:
            raise TransitionError(f"illegal or duplicate transition stage: {stage}")
        requirements = {
            "transition_started": set(),
            "input_committed": {"transition_started"},
            "fill_prepared": {"input_committed"},
            "fill_committed": {"fill_prepared"},
            "portfolio_committed": {"input_committed"},
            "strategy_committed": {"input_committed"},
            "order_submitted": {"strategy_committed"},
            "account_committed": {"strategy_committed", "portfolio_committed"},
            "risk_committed": {"account_committed"},
            "transition_committed": {"risk_committed"},
        }
        if not requirements[stage].issubset(self._seen):
            missing = sorted(requirements[stage] - self._seen)
            raise TransitionError(
                f"illegal transition stage order: {stage} requires {', '.join(missing)}"
            )
        if stage == "portfolio_committed" and "fill_prepared" in self._seen and "fill_committed" not in self._seen:
            raise TransitionError("portfolio commit requires the prepared fill to be committed")
        if stage == "strategy_committed" and "fill_committed" in self._seen and "portfolio_committed" not in self._seen:
            raise TransitionError("strategy cannot precede a carried fill's portfolio effect")
        self._seen.add(stage)

    def complete(self) -> None:
        if "transition_committed" not in self._seen:
            raise TransitionError("transition did not commit")


@dataclass(frozen=True, slots=True)
class TransitionCounters:
    inputs: int = 0
    orders: int = 0
    fills: int = 0
    journal_events: int = 0


@dataclass(frozen=True, slots=True)
class PaperTransitionState:
    """Non-restorable canonical state exposed to status projection."""

    input_cursor: int
    pending_order: object | None
    portfolio_snapshot: object
    account_snapshot: object
    risk_snapshot: object
    counters: TransitionCounters
    journal_snapshot: JournalSnapshot
    last_committed_ordering_key: str | None


def deterministic_id(run_id: str, input_identity: str, kind: str, domain: object) -> str:
    encoded = json.dumps(
        {"run_id": run_id, "input": input_identity, "kind": kind, "domain": domain},
        sort_keys=True, separators=(",", ":"), ensure_ascii=True,
    ).encode("ascii")
    return f"{kind}-{hashlib.sha256(encoded).hexdigest()[:24]}"


class PaperTransitionCoordinator:
    """Consume exactly one normalized bar through the canonical authorities."""

    def __init__(self, run_id: str, config: ProductConfig, strategy: Strategy,
                 journal: TransitionJournal,
                 failure_injector: Callable[[str], None] | None = None,
                 *, config_digest: str | None = None,
                 data_fingerprint: str | None = None,
                 restore_checkpoint: bool = True,
                 _publish_checkpoints: bool = True) -> None:
        if not run_id.strip():
            raise TransitionError("run ID must be non-empty")
        self.run_id, self.config, self.strategy, self.journal = run_id, config, strategy, journal
        self._failure_injector = failure_injector or (lambda boundary: None)
        self.config_digest = config_digest or _digest_value(config.normalized())
        if data_fingerprint is not None:
            if (not isinstance(data_fingerprint, str)
                    or not data_fingerprint.startswith("sha256:")
                    or len(data_fingerprint) != 71
                    or any(c not in "0123456789abcdef" for c in data_fingerprint[7:])):
                raise TransitionError("explicit data fingerprint must be a verified SHA-256")
            self.data_fingerprint = data_fingerprint
        else:
            self.data_fingerprint = _data_fingerprint(config)
        self._publish_checkpoints = _publish_checkpoints
        self._bus = EventBus()
        self._clock = {"value": None}
        self._paper = PaperExecutionEngine(self._bus, lambda: self._clock["value"])
        self._ledger = PortfolioLedger(self._bus)
        self._account = AccountEquityEngine(self._bus, config.starting_equity)
        self._portfolio_risk = PortfolioRiskEngine(self._bus, PortfolioRiskLimits(
            config.risk.require_positive_equity, config.risk.max_gross_notional,
            config.risk.max_abs_net_notional, config.risk.max_position_notional,
            config.risk.max_concentration_ratio, config.risk.max_gross_exposure_multiple,
            config.risk.max_drawdown))
        self._closes: list[float] = []
        self._pending = None
        self._cash_flow = 0.0
        self._cursor = 0
        self._orders = self._fills = self._events = 0
        self._poisoned = False
        self._last_committed_timestamp: datetime | None = None
        with RunDirectoryLock(journal.run_directory):
            snapshot = journal._snapshot_held()
            if snapshot.tail is not None and snapshot.tail.run_id != run_id:
                raise TransitionError("journal/lifecycle run ID mismatch")
            if snapshot.tail is not None:
                if not restore_checkpoint:
                    raise TransitionError("non-empty journal requires authoritative restoration")
                self._restore_held(snapshot)
                return
        self._journal_snapshot = snapshot
        self._portfolio_snapshot = self._ledger.snapshot()
        self._account_snapshot = self._account.value(self._portfolio_snapshot, ())
        self._risk_snapshot = self._portfolio_risk.evaluate(self._account_snapshot)

    def _restore_held(self, snapshot: JournalSnapshot) -> None:
        """Restore bounded Product authority without replaying historical transitions."""
        try:
            store = CheckpointStore(self.journal.run_directory)
            checkpoint_bytes = store.path.read_bytes()
            value = store.read()
            lifecycle_before = Lifecycle(self.journal.run_directory).current()
            _validate_checkpoint(value, self, snapshot, lifecycle_before)
            self._install_bounded_state(value)
            if (store.path.read_bytes() != checkpoint_bytes
                    or self.journal._snapshot_held() != snapshot
                    or Lifecycle(self.journal.run_directory).current() != lifecycle_before):
                raise CheckpointError("authority changed during restoration")
            self._journal_snapshot = snapshot
        except (CheckpointError, KeyError, OSError, TypeError, ValueError) as exc:
            raise TransitionError(f"authoritative checkpoint restoration failed: {exc}") from exc

    def checkpoint_document(self) -> dict[str, object]:
        tail = self._journal_snapshot.tail
        lifecycle = Lifecycle(self.journal.run_directory).current()
        if tail is None:
            raise TransitionError("cannot checkpoint an empty journal")
        return {
            "schema_version": 1, "run_id": self.run_id,
            "checkpoint_sequence": self._cursor,
            "config_digest": self.config_digest, "data_fingerprint": self.data_fingerprint,
            "cursor": self._cursor,
            "last_committed_ordering_key": self.state.last_committed_ordering_key,
            "strategy": {"name": self.strategy.name, "version": self.strategy.version,
                         "lookback_bound": _strategy_bound(self.strategy),
                         "closes": list(self._closes)},
            "execution": {"pending_order_id": (self._pending.order.order_id
                                                 if self._pending else None),
                          "pending": _pending_payload(self._pending),
                          "orders": self._orders, "fills": self._fills},
            "portfolio": _portfolio_authority(self._portfolio_snapshot),
            "account": _account_authority(self._account_snapshot, self._cash_flow),
            "risk": _risk_authority(self._risk_snapshot, max(
                (x.account_snapshot.equity for x in self._portfolio_risk.history()),
                default=self.config.starting_equity)),
            "counters": {"inputs": self._cursor, "orders": self._orders,
                         "fills": self._fills, "journal_events": self._events},
            "journal": {"sequence": tail.sequence, "digest": tail.digest,
                        "product_transition_id": tail.product_transition_id,
                        "input_cursor": tail.input_cursor},
            "lifecycle": {"run_id": lifecycle.run_id, "state": lifecycle.state.value,
                          "sequence": lifecycle.sequence},
            "recovery_counter": 0,
        }

    def _install_bounded_state(self, value: dict[str, object]) -> None:
        """Validate and install exact bounded reconstruction inputs."""
        strategy = value["strategy"]
        execution = value["execution"]
        counters = value["counters"]
        account = value["account"]
        risk = value["risk"]
        if not all(isinstance(x, dict) for x in (strategy, execution, counters, account, risk)):
            raise CheckpointError("invalid bounded authority sections")
        closes = strategy.get("closes")
        if (not isinstance(closes, list) or len(closes) > _strategy_bound(self.strategy)
                or not all(isinstance(x, (int, float)) and not isinstance(x, bool)
                           and isfinite(x) for x in closes)):
            raise CheckpointError("invalid bounded strategy state")
        self._closes = [float(x) for x in closes]
        self._cursor = _nonnegative_int(value["cursor"], "cursor")
        self._orders = _nonnegative_int(execution.get("orders"), "orders")
        self._fills = _nonnegative_int(execution.get("fills"), "fills")
        self._events = _nonnegative_int(counters.get("journal_events"), "journal events")
        if counters != {"inputs": self._cursor, "orders": self._orders,
                        "fills": self._fills, "journal_events": self._events}:
            raise CheckpointError("checkpoint counters mismatch")
        self._cash_flow = _finite_number(account.get("cash_flow"), "cash flow")
        self._last_committed_timestamp = datetime.fromisoformat(
            str(value["last_committed_ordering_key"]).replace("Z", "+00:00"))
        portfolio = _portfolio_from_authority(value["portfolio"])
        self._ledger.restore_checkpoint(portfolio)
        self._portfolio_snapshot = self._ledger.snapshot()
        mark_price = account.get("mark_price")
        marks = (() if not portfolio.positions else (MarketDataRecord(
            MarketDataKind.MARK_PRICE, portfolio.positions[0].symbol,
            portfolio.positions[0].source, self._last_committed_timestamp,
            {"price": _finite_number(mark_price, "mark price")}),))
        self._account_snapshot = self._account.value(portfolio, marks, cash_flow=self._cash_flow)
        self._portfolio_risk.restore_checkpoint_peak(
            _finite_number(risk.get("peak_equity"), "peak equity"))
        self._risk_snapshot = self._portfolio_risk.evaluate(self._account_snapshot)
        if _account_authority(self._account_snapshot, self._cash_flow) != account:
            raise CheckpointError("checkpoint account authority mismatch")
        if _risk_authority(self._risk_snapshot, risk["peak_equity"]) != risk:
            raise CheckpointError("checkpoint risk authority mismatch")
        self._pending = _restore_pending(execution.get("pending"), self)
        pending_id = self._pending.order.order_id if self._pending else None
        if pending_id != execution.get("pending_order_id"):
            raise CheckpointError("checkpoint pending execution mismatch")

    @property
    def state(self) -> PaperTransitionState:
        return PaperTransitionState(
            self._cursor, self._pending, self._portfolio_snapshot,
            self._account_snapshot, self._risk_snapshot,
            TransitionCounters(self._cursor, self._orders, self._fills, self._events),
            self._journal_snapshot,
            (self._last_committed_timestamp.isoformat().replace("+00:00", "Z")
             if self._last_committed_timestamp is not None else None),
        )

    def status_projection(self) -> dict[str, object]:
        """Build a non-authoritative view directly from the canonical state."""
        state = self.state
        return {
            "authoritative": False,
            "run_id": self.run_id,
            "input_cursor": state.input_cursor,
            "last_committed_ordering_key": state.last_committed_ordering_key,
            "pending_order": (
                state.pending_order.order.order_id if state.pending_order is not None else None
            ),
            "positions": [
                {"source": p.source, "symbol": p.symbol,
                 "quantity": p.signed_quantity, "average_entry_price": p.average_entry_price}
                for p in state.portfolio_snapshot.positions
            ],
            "equity": state.account_snapshot.equity,
            "risk_outcome": state.risk_snapshot.outcome.value,
            "counters": {
                "inputs": state.counters.inputs, "orders": state.counters.orders,
                "fills": state.counters.fills,
                "journal_events": state.counters.journal_events,
            },
        }

    def transition(self, bar: Bar) -> PaperTransitionState:
        """Commit one event while holding the run-directory writer lock throughout."""
        if not isinstance(bar, Bar):
            raise TransitionError("input must be one normalized Bar")
        if self._poisoned:
            raise TransitionError("coordinator is unusable after a partial transition")
        with RunDirectoryLock(self.journal.run_directory):
            try:
                return self._transition_held(bar)
            except BaseException:
                # Checkpoint 4 will restore authorities.  Until then, none of
                # the canonical engines may be reused after an uncertain cut.
                self._poisoned = True
                raise

    def _transition_held(self, bar: Bar) -> PaperTransitionState:
        persisted = self.journal._snapshot_held()
        if persisted != self._journal_snapshot:
            raise TransitionError("journal advanced outside this coordinator")
        try:
            lifecycle = Lifecycle(self.journal.run_directory).current()
        except LifecycleError as exc:
            raise TransitionError("valid lifecycle authority is required for a market transition") from exc
        if lifecycle.run_id != self.run_id or lifecycle.state is not LifecycleState.RUNNING:
            raise TransitionError("lifecycle is not eligible for a market transition")
        if bar.timestamp.tzinfo is None or bar.timestamp.utcoffset() is None:
            raise TransitionError("market timestamp must be timezone-aware")
        if (self._last_committed_timestamp is not None
                and bar.timestamp <= self._last_committed_timestamp):
            raise TransitionError("market timestamps must be strictly increasing")
        cursor = self._cursor + 1
        timestamp = bar.timestamp.isoformat().replace("+00:00", "Z")
        input_domain = {"cursor": cursor, "timestamp": timestamp, "open": bar.open,
                        "high": bar.high, "low": bar.low, "close": bar.close,
                        "volume": bar.volume, "funding_rate": bar.funding_rate}
        input_id = deterministic_id(
            self.run_id, timestamp, "input",
            {key: value for key, value in input_domain.items() if key != "cursor"},
        )
        transition_id = deterministic_id(self.run_id, input_id, "transition", cursor)
        protocol = StageProtocol()

        def emit(stage: str, payload: dict[str, object]) -> None:
            protocol.accept(stage)
            self._failure_injector(f"journal:{stage}")
            record = self.journal._append_held(
                self.run_id, transition_id, stage, "paper_market_transition", timestamp,
                cursor if stage == "transition_committed" else self._cursor,
                {"input_event_id": input_id, **payload},
            )
            self._events += 1

        emit("transition_started", {})
        emit("input_committed", input_domain)
        self._clock["value"] = bar.timestamp
        carried = _position(self._ledger, self.config.data.source, self.config.data.symbol)
        funding = -carried * bar.close * bar.funding_rate if bar.funding_rate is not None else 0.0
        commission = 0.0

        pending_fill: tuple[str, str, float, float] | None = None
        if self._pending is not None:
            intent = self._pending
            signed = intent.order.quantity * (1 if intent.order.side.value == "buy" else -1)
            fill_price = bar.open * (1 + (1 if signed > 0 else -1) * self.config.costs.slippage_bps / 10000)
            fill_id = deterministic_id(self.run_id, input_id, "fill", intent.order.order_id)
            emit("fill_prepared", {"fill_id": fill_id, "order_id": intent.order.order_id,
                                   "fill_price": fill_price})
            self._failure_injector("fill")
            report = self._paper.fill(intent.order.order_id, fill_price)
            emit("fill_committed", {"fill_id": fill_id, "order_id": report.order.order_id,
                                    "quantity": signed, "fill_price": fill_price})
            self._failure_injector("portfolio")
            self._ledger.apply(report)
            commission = abs(signed * fill_price) * self.config.costs.commission_bps / 10000
            pending_fill = (fill_id, report.order.order_id, fill_price, commission)
            self._fills += 1
            self._pending = None
            self._portfolio_snapshot = self._ledger.snapshot()
            emit("portfolio_committed", {"positions": _positions_payload(self._portfolio_snapshot)})

        current = _position(self._ledger, self.config.data.source, self.config.data.symbol)
        self._closes.append(bar.close)
        del self._closes[:-_strategy_bound(self.strategy)]
        self._failure_injector("strategy")
        normalized = self.strategy.target(StrategyContext(bar, tuple(self._closes),
                                                           current / self.config.risk.max_position))
        if (isinstance(normalized, bool) or not isinstance(normalized, (int, float))
                or not isfinite(normalized) or not -1 <= normalized <= 1):
            raise TransitionError("strategy target must be finite and normalized")
        target, requested = float(normalized) * self.config.risk.max_position, 0.0
        requested = target - current
        emit("strategy_committed", {"normalized_target": float(normalized), "target": target})

        if requested:
            direction = AlphaDirection.LONG if requested > 0 else AlphaDirection.SHORT
            decision = DecisionEngine(self._bus, ThresholdDecisionPolicy(
                clock=lambda timestamp=bar.timestamp: timestamp)).decide(
                    _candidate(self.config, bar, direction))
            assessment = RiskEngine(self._bus, ThresholdRiskPolicy(
                clock=lambda timestamp=bar.timestamp: timestamp)).assess(decision)
            order_id = deterministic_id(self.run_id, input_id, "order", {"quantity": requested})
            execution = ExecutionEngine(self._bus, FixedQuantityExecutionPolicy(
                abs(requested), clock=lambda timestamp=bar.timestamp: timestamp,
                order_id_factory=lambda: order_id))
            intent = execution.create_intent(assessment)
            self._failure_injector("submit")
            self._paper.submit(intent)
            self._orders += 1
            emit("order_submitted", {"order_id": order_id, "quantity": requested})
            if self.config.fill_timing == "next_open":
                self._pending = intent
            else:
                fill_price = bar.close * (1 + (1 if requested > 0 else -1) * self.config.costs.slippage_bps / 10000)
                fill_id = deterministic_id(self.run_id, input_id, "fill", order_id)
                emit("fill_prepared", {"fill_id": fill_id, "order_id": order_id,
                                       "fill_price": fill_price})
                self._failure_injector("fill")
                report = self._paper.fill(order_id, fill_price)
                emit("fill_committed", {"fill_id": fill_id, "order_id": order_id,
                                        "quantity": requested, "fill_price": fill_price})
                self._failure_injector("portfolio")
                self._ledger.apply(report)
                commission += abs(requested * fill_price) * self.config.costs.commission_bps / 10000
                self._fills += 1
        if pending_fill is None:
            self._portfolio_snapshot = self._ledger.snapshot()
            emit("portfolio_committed", {"positions": _positions_payload(self._portfolio_snapshot)})
        marks = tuple(MarketDataRecord(
            MarketDataKind.MARK_PRICE, p.symbol, p.source, bar.timestamp, {"price": bar.close})
            for p in self._portfolio_snapshot.positions if p.side is not PositionSide.FLAT)
        self._cash_flow += funding - commission
        self._failure_injector("account")
        self._account_snapshot = self._account.value(
            self._portfolio_snapshot, marks, cash_flow=self._cash_flow)
        emit("account_committed", {"equity": self._account_snapshot.equity,
                                   "cash_flow": self._cash_flow, "commission": commission,
                                   "funding": funding,
                                   "unrealized_pnl": self._account_snapshot.total_unrealized_pnl})
        self._failure_injector("risk")
        self._risk_snapshot = self._portfolio_risk.evaluate(self._account_snapshot)
        emit("risk_committed", {"outcome": self._risk_snapshot.outcome.value,
                                "drawdown": self._risk_snapshot.drawdown_ratio,
                                "breaches": [b.code.value for b in self._risk_snapshot.breaches]})
        emit("transition_committed", {"orders": self._orders, "fills": self._fills})
        protocol.complete()
        self._cursor = cursor
        self._last_committed_timestamp = bar.timestamp
        self._journal_snapshot = self.journal._snapshot_held()
        if self._publish_checkpoints:
            self._failure_injector("checkpoint")
            CheckpointStore(self.journal.run_directory)._write_held(self.checkpoint_document())
        return self.state


def _positions_payload(snapshot: object) -> list[dict[str, object]]:
    return [
        {"source": position.source, "symbol": position.symbol,
         "quantity": position.signed_quantity,
         "average_entry_price": position.average_entry_price}
        for position in snapshot.positions
    ]


def _time(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _strategy_bound(strategy: Strategy) -> int:
    """Fixed documented maximum close window required by Product v0.1 strategies."""
    if strategy.name == "moving_average_crossover":
        return int(getattr(strategy, "slow"))
    if strategy.name == "channel_breakout":
        return int(getattr(strategy, "lookback")) + 1
    return 1


def _portfolio_authority(snapshot: PortfolioSnapshot) -> list[dict[str, object]]:
    return [{"source": p.source, "symbol": p.symbol, "quantity": p.signed_quantity,
             "side": p.side.value, "average_entry_price": p.average_entry_price,
             "realized_pnl": p.realized_pnl, "updated_at": _time(p.updated_at),
             "last_order_id": p.last_order_id} for p in snapshot.positions]


def _portfolio_from_authority(value: object) -> PortfolioSnapshot:
    if not isinstance(value, list):
        raise CheckpointError("invalid portfolio authority")
    positions = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"source", "symbol", "quantity", "side",
                "average_entry_price", "realized_pnl", "updated_at", "last_order_id"}:
            raise CheckpointError("invalid portfolio position authority")
        positions.append(PositionSnapshot(
            item["source"], item["symbol"], item["quantity"], PositionSide(item["side"]),
            item["average_entry_price"], item["realized_pnl"],
            datetime.fromisoformat(str(item["updated_at"]).replace("Z", "+00:00")),
            item["last_order_id"]))
    return PortfolioLedger._make_snapshot({(p.source, p.symbol): p for p in positions})


def _account_authority(snapshot: object, cash_flow: float) -> dict[str, object]:
    return {"equity": snapshot.equity, "cash_flow": cash_flow,
            "total_realized_pnl": snapshot.total_realized_pnl,
            "total_unrealized_pnl": snapshot.total_unrealized_pnl,
            "mark_price": (snapshot.valuations[0].mark_price if snapshot.valuations else None),
            "valued_at": _time(snapshot.valued_at)}


def _risk_authority(snapshot: object, peak: object) -> dict[str, object]:
    return {"outcome": snapshot.outcome.value, "drawdown": snapshot.drawdown_ratio,
            "peak_equity": peak}


def _pending_payload(intent: object | None) -> dict[str, object] | None:
    if intent is None:
        return None
    observation = intent.risk_assessment.decision_intent.alpha_candidate.observation
    signed = intent.order.quantity * (1 if intent.order.side.value == "buy" else -1)
    return {"order_id": intent.order.order_id, "quantity": signed,
            "timestamp": _time(observation.timestamp), "price": observation.price}


def _restore_pending(value: object, coordinator: PaperTransitionCoordinator) -> object | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {"order_id", "quantity", "timestamp", "price"}:
        raise CheckpointError("invalid pending execution authority")
    quantity = _finite_number(value["quantity"], "pending quantity")
    timestamp = datetime.fromisoformat(str(value["timestamp"]).replace("Z", "+00:00"))
    price = _finite_number(value["price"], "pending price")
    direction = AlphaDirection.LONG if quantity > 0 else AlphaDirection.SHORT
    synthetic = Bar(timestamp, price, price, price, price, 0.0, None)
    decision = DecisionEngine(coordinator._bus, ThresholdDecisionPolicy(
        clock=lambda: timestamp)).decide(_candidate(coordinator.config, synthetic, direction))
    assessment = RiskEngine(coordinator._bus, ThresholdRiskPolicy(
        clock=lambda: timestamp)).assess(decision)
    intent = ExecutionEngine(coordinator._bus, FixedQuantityExecutionPolicy(
        abs(quantity), clock=lambda: timestamp,
        order_id_factory=lambda: value["order_id"])).create_intent(assessment)
    coordinator._clock["value"] = timestamp
    coordinator._paper.submit(intent)
    return intent


def _finite_number(value: object, name: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool) or not isfinite(value):
        raise CheckpointError(f"invalid checkpoint {name}")
    return float(value)


def _nonnegative_int(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise CheckpointError(f"invalid checkpoint {name}")
    return value


def _digest_value(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=True).encode("ascii")).hexdigest()


def _data_fingerprint(config: ProductConfig) -> str:
    """Bind local replay identities to content, not merely to a pathname."""
    path = Path(config.data.path)
    if not path.is_file():
        raise TransitionError("replay data requires a readable regular file or explicit verified fingerprint")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise TransitionError("replay data content cannot be verified") from exc
    return _digest_value({
        "path": config.data.path, "source": config.data.source,
        "symbol": config.data.symbol, "timeframe": config.data.timeframe,
        "content_sha256": digest.hexdigest(),
    })


def _bar_to_dict(bar: Bar) -> dict[str, object]:
    return {"timestamp": bar.timestamp.isoformat().replace("+00:00", "Z"),
            "open": bar.open, "high": bar.high, "low": bar.low, "close": bar.close,
            "volume": bar.volume, "funding_rate": bar.funding_rate}


def _bar_from_dict(value: object) -> Bar:
    if not isinstance(value, dict) or set(value) != {
            "timestamp", "open", "high", "low", "close", "volume", "funding_rate"}:
        raise CheckpointError("invalid checkpoint market history")
    return Bar(datetime.fromisoformat(str(value["timestamp"]).replace("Z", "+00:00")),
               value["open"], value["high"], value["low"], value["close"],
               value["volume"], value["funding_rate"])  # type: ignore[arg-type]


def _validate_checkpoint(value: dict[str, object], coordinator: PaperTransitionCoordinator,
                         snapshot: JournalSnapshot, lifecycle: object) -> None:
    fields = {"schema_version", "run_id", "checkpoint_sequence", "config_digest",
              "data_fingerprint", "cursor", "last_committed_ordering_key", "strategy",
              "execution", "portfolio", "account", "risk", "counters",
              "journal", "lifecycle", "recovery_counter"}
    if set(value) != fields or value["schema_version"] != 1:
        raise CheckpointError("unsupported checkpoint schema")
    if value["run_id"] != coordinator.run_id:
        raise CheckpointError("checkpoint run ID mismatch")
    if value["config_digest"] != coordinator.config_digest:
        raise CheckpointError("checkpoint config digest mismatch")
    if value["data_fingerprint"] != coordinator.data_fingerprint:
        raise CheckpointError("checkpoint data fingerprint mismatch")
    cursor = value["cursor"]
    if not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 1 or value["checkpoint_sequence"] != cursor:
        raise CheckpointError("invalid checkpoint cursor")
    strategy = value["strategy"]
    if not isinstance(strategy, dict) or strategy.get("name") != coordinator.strategy.name or strategy.get("version") != coordinator.strategy.version:
        raise CheckpointError("unsupported strategy identity or version")
    if strategy.get("lookback_bound") != _strategy_bound(coordinator.strategy):
        raise CheckpointError("strategy lookback mismatch")
    tail = snapshot.tail
    if tail is None or value["journal"] != {"sequence": tail.sequence, "digest": tail.digest,
                                            "product_transition_id": tail.product_transition_id,
                                            "input_cursor": tail.input_cursor}:
        raise CheckpointError("checkpoint journal lineage mismatch")
    if value["lifecycle"] != {"run_id": lifecycle.run_id, "state": lifecycle.state.value,
                              "sequence": lifecycle.sequence} or lifecycle.state is not LifecycleState.RUNNING:
        raise CheckpointError("checkpoint lifecycle mismatch")
    if value["last_committed_ordering_key"] != tail.effective_market_timestamp:
        raise CheckpointError("checkpoint ordering key mismatch")
    if value["recovery_counter"] != 0:
        raise CheckpointError("invalid recovery counter")
