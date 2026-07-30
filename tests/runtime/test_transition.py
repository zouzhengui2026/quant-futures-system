from datetime import datetime, timedelta, timezone

import pytest

from quant_futures.paper_runtime import PaperTransitionCoordinator, StageProtocol, TransitionError
from quant_futures.paper_runtime.journal import TransitionJournal
from quant_futures.product.config import CostConfig, DataConfig, ProductConfig
from quant_futures.product.data import Bar
from quant_futures.product.engine import simulate
from quant_futures.product.strategy import FixedStrategy, HoldStrategy


def config(*, timing="current_close", commission=0.0, slippage=0.0):
    return ProductConfig("paper", DataConfig("unused.csv"),
                         costs=CostConfig(commission, slippage, 0.0),
                         fill_timing=timing)


def bar(n, *, close=100.0, open_=100.0, funding=None):
    timestamp = datetime(2025, 1, 1, tzinfo=timezone.utc) + timedelta(hours=n)
    return Bar(timestamp, open_, max(open_, close), min(open_, close), close, 1.0, funding)


def stages(journal):
    return [record.stage for record in journal.iter_records()]


def test_no_order_transition_and_status_use_canonical_state(tmp_path):
    journal = TransitionJournal(tmp_path)
    coordinator = PaperTransitionCoordinator("run-a", config(), HoldStrategy(), journal)
    state = coordinator.transition(bar(0))
    assert stages(journal) == [
        "transition_started", "input_committed", "strategy_committed",
        "portfolio_committed", "account_committed", "risk_committed",
        "transition_committed",
    ]
    assert state.input_cursor == 1
    assert coordinator.status_projection()["equity"] == state.account_snapshot.equity
    assert coordinator.status_projection()["positions"] == []


def test_current_close_costs_funding_and_canonical_outputs(tmp_path):
    coordinator = PaperTransitionCoordinator(
        "run-b", config(commission=10.0, slippage=10.0), FixedStrategy(1.0),
        TransitionJournal(tmp_path))
    first = coordinator.transition(bar(0))
    second = coordinator.transition(bar(1, close=110.0, funding=0.01))
    position = second.portfolio_snapshot.positions[0]
    assert position.signed_quantity == 1.0
    assert position.average_entry_price == pytest.approx(100.1)
    # Both fee and explicit funding flow through AccountEquityEngine cash_flow.
    assert second.account_snapshot.cash_flow == pytest.approx(-1.2001)
    assert second.risk_snapshot.account_snapshot is second.account_snapshot
    assert first.counters.fills == 1 and second.counters.fills == 1


def test_matches_product_v01_account_portfolio_and_risk_path(tmp_path):
    cfg = config(commission=2.0, slippage=1.0)
    bars = (bar(0), bar(1, close=110.0, funding=0.001))
    expected = simulate(cfg, bars, FixedStrategy(1.0))
    coordinator = PaperTransitionCoordinator(
        "canonical-comparison", cfg, FixedStrategy(1.0), TransitionJournal(tmp_path))
    states = tuple(coordinator.transition(item) for item in bars)
    for state, record in zip(states, expected):
        assert state.account_snapshot.equity == record.equity
        assert state.portfolio_snapshot.positions[0].signed_quantity == record.quantity
        assert state.risk_snapshot.drawdown_ratio == record.drawdown


def test_next_open_creates_then_fills_pending_order(tmp_path):
    journal = TransitionJournal(tmp_path)
    coordinator = PaperTransitionCoordinator(
        "run-c", config(timing="next_open"), FixedStrategy(1.0), journal)
    first = coordinator.transition(bar(0))
    assert first.pending_order is not None and first.counters.fills == 0
    second = coordinator.transition(bar(1, open_=105.0, close=106.0))
    assert second.pending_order is None and second.counters.fills == 1
    assert second.portfolio_snapshot.positions[0].average_entry_price == 105.0


def test_stage_protocol_rejects_illegal_and_duplicate_stages():
    protocol = StageProtocol()
    with pytest.raises(TransitionError):
        protocol.accept("strategy_committed")
    protocol.accept("transition_started")
    with pytest.raises(TransitionError):
        protocol.accept("transition_started")


def test_deterministic_byte_stable_journal(tmp_path):
    paths = [tmp_path / "one", tmp_path / "two"]
    payloads = []
    for path in paths:
        path.mkdir()
        coordinator = PaperTransitionCoordinator(
            "same-run", config(), FixedStrategy(1.0), TransitionJournal(path))
        coordinator.transition(bar(0))
        payloads.append((path / "transitions.journal").read_bytes())
    assert payloads[0] == payloads[1]


def test_cursor_does_not_advance_when_commit_append_fails(tmp_path, monkeypatch):
    journal = TransitionJournal(tmp_path)
    coordinator = PaperTransitionCoordinator("run-d", config(), HoldStrategy(), journal)
    original = journal.append

    def fail_commit(*args, **kwargs):
        if args[2] == "transition_committed":
            raise OSError("durability failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(journal, "append", fail_commit)
    with pytest.raises(OSError):
        coordinator.transition(bar(0))
    assert coordinator.state.input_cursor == 0
