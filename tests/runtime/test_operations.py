import json
import signal
import multiprocessing
import pytest

from quant_futures.paper_runtime import (OperationalRequests, PaperRuntime,
                                         RecoveryAttempts, StopFlag)
from quant_futures.paper_runtime.journal import TransitionJournal
from quant_futures.product.strategy import FixedStrategy

from test_transition import authorized_coordinator, bar, config


def _hold_consumer_lease(directory, ready):
    from quant_futures.paper_runtime import RuntimeConsumerLease
    lease = RuntimeConsumerLease(directory).acquire()
    ready.set()
    signal.pause()
    lease.release()


def test_consumer_lease_is_process_lifetime_and_status_detects_owner_death(tmp_path):
    """A killed owner relinquishes the lease and RUNNING becomes stalled."""
    from quant_futures.paper_runtime import RuntimeConsumerLease, RunLockError
    from quant_futures.paper_runtime import control as paper_control

    run = paper_control.start(tmp_path / "runs")
    ready = multiprocessing.Event()
    process = multiprocessing.Process(target=_hold_consumer_lease, args=(run, ready))
    process.start()
    assert ready.wait(5)
    assert paper_control.project_status(run)["consumer_owner"] == "live"
    with pytest.raises(RunLockError):
        RuntimeConsumerLease(run).acquire()
    process.kill(); process.join(5)
    assert process.exitcode is not None
    status = paper_control.project_status(run)
    assert status["consumer_owner"] == "relinquished"
    assert status["health"] == "stalled"
    assert status["stalled"] is True


def test_recover_fails_before_mutation_while_live_consumer_owns_run(tmp_path):
    from quant_futures.paper_runtime import RunLockError
    from quant_futures.paper_runtime import control as paper_control

    run = paper_control.start(tmp_path / "runs")
    ready = multiprocessing.Event()
    process = multiprocessing.Process(target=_hold_consumer_lease, args=(run, ready))
    process.start(); assert ready.wait(5)
    lifecycle = (run / "lifecycle.jsonl").read_bytes()
    with pytest.raises(RunLockError):
        paper_control.recover(run)
    assert (run / "lifecycle.jsonl").read_bytes() == lifecycle
    assert not (run / "recovery-attempts.jsonl").exists()
    process.kill(); process.join(5)


@pytest.mark.parametrize("kind", ["unacquired", "wrong-directory", "released", "reused"])
def test_supplied_consumer_lease_cannot_bypass_live_owner(tmp_path, kind):
    """Caller-supplied objects must prove a live, exact-directory capability."""
    from quant_futures.paper_runtime import RunLockError, RuntimeConsumerLease

    run = tmp_path / "run"
    run.mkdir()
    coordinator = authorized_coordinator(
        f"lease-{kind}", config(), FixedStrategy(0.0), TransitionJournal(run))
    ready = multiprocessing.Event()
    owner = multiprocessing.Process(target=_hold_consumer_lease, args=(run, ready))
    owner.start(); assert ready.wait(5)
    authority_names = ("lifecycle.jsonl", "transitions.journal", "checkpoint.json")
    before = {name: (run / name).read_bytes() if (run / name).exists() else None
              for name in authority_names}

    if kind == "wrong-directory":
        (tmp_path / "other").mkdir()
        supplied = RuntimeConsumerLease(tmp_path / "other").acquire()
    else:
        supplied = RuntimeConsumerLease(run)
        if kind in {"released", "reused"}:
            owner.kill(); owner.join(5)
            supplied.acquire(); supplied.release()

    with pytest.raises(RunLockError, match="not held for this run directory"):
        PaperRuntime(coordinator, consumer_lease=supplied).run((bar(0),))
    after = {name: (run / name).read_bytes() if (run / name).exists() else None
             for name in authority_names}
    assert after == before

    supplied.release()
    if owner.is_alive():
        owner.kill(); owner.join(5)


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


def test_committed_pause_terminates_runtime_ownership(tmp_path):
    """PAUSED is reopenable because the old consumer has already returned."""
    from quant_futures.paper_runtime import Lifecycle, LifecycleState

    coordinator = authorized_coordinator(
        "pause-owner", config(), FixedStrategy(0.0), TransitionJournal(tmp_path))
    OperationalRequests(tmp_path).request("pause")
    result = PaperRuntime(coordinator).run((bar(0), bar(1)))

    assert result.stopped
    assert result.processed == 0
    assert result.state.input_cursor == 0
    assert Lifecycle(tmp_path).current().state is LifecycleState.PAUSED


def _runtime_files(root):
    replay = root / "bars.csv"
    replay.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2025-01-01T00:00:00Z,100,101,99,100,1\n"
        "2025-01-01T01:00:00Z,100,102,99,101,1\n", encoding="utf-8")
    config_path = root / "paper.yml"
    config_path.write_text(
        "mode: paper\n"
        "data:\n  path: bars.csv\n  source: test\n  symbol: BTC\n  timeframe: 1h\n"
        "strategy:\n  name: flat\n  parameters: {}\n"
        "fill_timing: current_close\noutput_directory: .\n", encoding="utf-8")
    return config_path, replay


def test_fresh_runtime_pause_resume_baselines_consumed_request(tmp_path):
    """A reopened runtime cannot replay the pause request consumed by its predecessor."""
    from dataclasses import replace
    from quant_futures.paper_runtime import Lifecycle, LifecycleState, PaperTransitionCoordinator
    from quant_futures.paper_runtime import control as paper_control
    from quant_futures.product.config import load_config
    from quant_futures.product.data import load_bars
    from quant_futures.product.strategy import build_strategy

    config_path, replay = _runtime_files(tmp_path)
    run = paper_control.start(tmp_path / "runs")
    paper_control.write_runtime_metadata(run, config_path, replay)
    cfg = load_config(config_path)
    cfg = replace(cfg, data=replace(cfg.data, path=str(replay)))
    bars, fingerprint = load_bars(replay, cfg.data.schema, timeframe=cfg.data.timeframe)
    lifecycle = Lifecycle(run)
    coordinator = PaperTransitionCoordinator(
        lifecycle.current().run_id, cfg,
        build_strategy(cfg.strategy.name, cfg.strategy.parameters),
        TransitionJournal(run), data_fingerprint=f"sha256:{fingerprint}")

    pause = OperationalRequests(run).request("pause")
    first = PaperRuntime(coordinator).run(bars)
    assert first.processed == 0
    assert lifecycle.current().state is LifecycleState.PAUSED

    resumed = paper_control.resume_runtime(run)
    assert resumed.processed == len(bars)
    assert lifecycle.current().state is LifecycleState.COMPLETED
    assert OperationalRequests(run)._read_held()["sequence"] == pause["sequence"] + 1


def test_paused_stop_completes_without_a_consumer(tmp_path):
    from quant_futures.paper_runtime import Lifecycle, LifecycleState
    from quant_futures.paper_runtime import control as paper_control

    config_path, replay = _runtime_files(tmp_path)
    run = paper_control.start(tmp_path / "runs")
    paper_control.write_runtime_metadata(run, config_path, replay)
    paper_control.transition(run, LifecycleState.PAUSED, "test relinquished pause")

    paper_control.request_stop(run)
    assert Lifecycle(run).current().state is LifecycleState.COMPLETED
    # Repeated stop is deterministic and does not add lifecycle records.
    before = (run / "lifecycle.jsonl").read_bytes()
    paper_control.request_stop(run)
    assert (run / "lifecycle.jsonl").read_bytes() == before


def test_recovery_attempt_authority_is_monotonic_and_hash_chained(tmp_path):
    attempts = RecoveryAttempts(tmp_path)
    assert attempts.append_held("run", "started") == 1
    assert attempts.append_held("run", "recovered") == 2
    records = attempts.read()
    assert [record["sequence"] for record in records] == [1, 2]
    assert records[1]["previous_digest"] == records[0]["digest"]


@pytest.mark.parametrize("cut", ["after_started", "after_suffix_checkpoint", "after_outcome"])
def test_recovery_publication_protocol_reconciles_each_durable_cut(tmp_path, cut):
    from quant_futures.paper_runtime.checkpoint import CheckpointStore
    from quant_futures.paper_runtime.control import (
        _publish_recovery_commitment_held, _reconcile_recovery_publication_held)

    store = CheckpointStore(tmp_path)
    store._write_held({"recovery_counter": 0, "recovery_digest": None})
    attempts = RecoveryAttempts(tmp_path)
    attempts.append_held("run", "started")
    if cut == "after_suffix_checkpoint":
        _publish_recovery_commitment_held(tmp_path)
    elif cut == "after_outcome":
        attempts.append_held("run", "recovered")

    _reconcile_recovery_publication_held(tmp_path, "run")
    records = attempts.read()
    assert [record["outcome"] for record in records] == [
        "started", "recovered" if cut == "after_suffix_checkpoint" else
        ("recovered" if cut == "after_outcome" else "failed")]
    checkpoint = store.read()
    assert checkpoint["recovery_counter"] == 1
    assert checkpoint["recovery_digest"] == records[-1]["digest"]

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
