import json
import signal
import pytest

from quant_futures.paper_runtime import (OperationalRequests, PaperRuntime,
                                         RecoveryAttempts, StopFlag)
from quant_futures.paper_runtime.journal import TransitionJournal
from quant_futures.product.strategy import FixedStrategy

from test_transition import authorized_coordinator, bar, config


def test_finite_runtime_stops_only_after_committed_boundary(tmp_path):
    coordinator = authorized_coordinator(
        "runtime", config(), FixedStrategy(1.0), TransitionJournal(tmp_path))
    flag = StopFlag()
    boundaries = []

    def inject(boundary):
        if boundary == "journal:strategy_committed":
            flag.handler(signal.SIGTERM, None)
            boundaries.append(coordinator.state.input_cursor)

    coordinator._failure_injector = inject
    result = PaperRuntime(coordinator, stop_flag=flag).run((bar(0), bar(1)))
    assert boundaries == [0]
    assert result.processed == 1
    assert result.state.input_cursor == 1
    assert result.stopped


def test_operational_requests_are_monotonic_and_idempotent_at_boundary(tmp_path):
    coordinator = authorized_coordinator(
        "requests", config(), FixedStrategy(0.0), TransitionJournal(tmp_path))
    requests = OperationalRequests(tmp_path)
    assert requests.request("resume")["sequence"] == 1
    assert requests.request("resume")["sequence"] == 2
    result = PaperRuntime(coordinator).run((bar(0),))
    assert result.processed == 1
    assert result.state.input_cursor == 1


def test_recovery_attempt_authority_is_monotonic_and_hash_chained(tmp_path):
    attempts = RecoveryAttempts(tmp_path)
    assert attempts.append_held("run", "started") == 1
    assert attempts.append_held("run", "recovered") == 2
    records = attempts.read()
    assert [record["sequence"] for record in records] == [1, 2]
    assert records[1]["previous_digest"] == records[0]["digest"]

@pytest.mark.parametrize("stage", [
    "transition_started", "input_committed", "strategy_committed", "order_submitted",
    "fill_prepared", "fill_committed", "portfolio_committed", "account_committed",
    "risk_committed", "transition_committed",
])
def test_fresh_process_recovery_completes_each_durable_current_close_prefix(tmp_path, stage):
    """A durable prefix is verified, never duplicated, and only its suffix is appended."""
    from quant_futures.paper_runtime import Lifecycle, LifecycleState, PaperTransitionCoordinator, TransitionJournal
    from quant_futures.paper_runtime import control as paper_control
    from quant_futures.product.config import load_config
    from quant_futures.product.data import load_bars
    from quant_futures.product.strategy import build_strategy

    replay = tmp_path / "bars.csv"
    replay.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2025-01-01T00:00:00Z,100,101,99,100,1\n"
        "2025-01-01T01:00:00Z,100,111,99,110,1\n", encoding="utf-8")
    config_path = tmp_path / "paper.yml"
    config_path.write_text(
        "mode: paper\n"
        "data:\n  path: bars.csv\n  source: test\n  symbol: BTC\n  timeframe: 1h\n"
        "strategy:\n  name: moving_average_crossover\n  parameters:\n    fast: 1\n    slow: 2\n"
        "fill_timing: current_close\noutput_directory: .\n", encoding="utf-8")
    config = load_config(config_path)
    bars, fingerprint = load_bars(replay, config.data.schema, timeframe="1h")

    def execute(directory, cut=None):
        run_id = Lifecycle(directory).current().run_id
        armed = {"value": False}
        def inject(boundary):
            if armed["value"] and cut and boundary == f"durable:{cut}":
                raise RuntimeError("simulated process cut")
        coordinator = PaperTransitionCoordinator(
            run_id, config, build_strategy(config.strategy.name, config.strategy.parameters),
            TransitionJournal(directory), failure_injector=inject,
            data_fingerprint=f"sha256:{fingerprint}")
        coordinator.transition(bars[0])
        armed["value"] = True
        if cut:
            with pytest.raises(RuntimeError, match="process cut"):
                coordinator.transition(bars[1])
        else:
            coordinator.transition(bars[1])

    def create(path):
        path.mkdir()
        lifecycle = Lifecycle(path); lifecycle.initialize("recovery-run")
        lifecycle.transition(LifecycleState.STARTING, "start")
        lifecycle.transition(LifecycleState.RUNNING, "run")
        return path

    expected = create(tmp_path / "expected")
    paper_control.write_runtime_metadata(expected, config_path, replay)
    execute(expected)
    # Compare authorities at the same operational recovery count.
    paper_control.recover(expected)
    paper_control.continue_runtime(expected)
    recovered = create(tmp_path / "recovered")
    paper_control.write_runtime_metadata(recovered, config_path, replay)
    execute(recovered, stage)
    paper_control.recover(recovered)
    paper_control.continue_runtime(recovered)

    records = TransitionJournal(recovered).records()
    second_id = records[-1].product_transition_id
    second = [record.stage for record in records
              if record.product_transition_id == second_id]
    assert len(second) == len(set(second))
    recovered_checkpoint = json.loads((recovered / "checkpoint.json").read_text())
    expected_checkpoint = json.loads((expected / "checkpoint.json").read_text())
    # The protected recovery outcome digests intentionally distinguish a
    # recovered invocation from a coherent no-op invocation.
    recovered_checkpoint.pop("recovery_digest")
    expected_checkpoint.pop("recovery_digest")
    assert recovered_checkpoint == expected_checkpoint
    assert [record.stage for record in records] == [
        record.stage for record in TransitionJournal(expected).records()]
