import json
import signal
import multiprocessing
import os
import subprocess
import sys
import time
from pathlib import Path
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


def _source_cli_env() -> tuple[Path, dict[str, str]]:
    repository = Path(__file__).parents[2]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(repository / "src")
    return repository, env


def _fresh_cli(*arguments: str, timeout: float = 20) -> subprocess.CompletedProcess[str]:
    """Run one control command in a new source-tree Python process."""
    repository, env = _source_cli_env()
    return subprocess.run(
        [sys.executable, "-m", "quant_futures.product.cli", *arguments],
        cwd=repository, env=env, text=True, capture_output=True, timeout=timeout,
    )


def _checkpoint_product_core(checkpoint):
    """Exclude only operational lineage while retaining all Product authority."""
    return {key: value for key, value in checkpoint.items()
            if key not in {"lifecycle", "recovery_counter", "recovery_digest"}}


def _assert_terminal_authority(run):
    from quant_futures.paper_runtime import (CheckpointStore, Lifecycle,
                                               LifecycleState,
                                               RuntimeConsumerLease)
    from quant_futures.paper_runtime import control as paper_control

    records = TransitionJournal(run).records()
    checkpoint = CheckpointStore(run).read()
    assert Lifecycle(run).current().state is LifecycleState.COMPLETED
    assert records[-1].stage == "transition_committed"
    assert (records[-1].sequence, records[-1].digest) == (
        checkpoint["journal"]["sequence"], checkpoint["journal"]["digest"])
    for identities in (
        [record.journal_event_id for record in records],
        [record.product_transition_id for record in records
         if record.stage == "transition_started"],
        [record.payload["input_event_id"] for record in records
         if record.stage == "transition_started"],
        [record.payload["order_id"] for record in records
         if record.stage == "order_submitted"],
        [record.payload["fill_id"] for record in records
         if record.stage == "fill_committed"],
    ):
        assert len(identities) == len(set(identities))
    status = paper_control.project_status(run)
    assert status["lifecycle"] == "COMPLETED"
    assert status["consumer_owner"] == "relinquished"
    assert paper_control.audit(run)
    assert json.loads((run / "status.json").read_text()) == status
    assert list(run.glob(".checkpoint.json.*.tmp")) == []
    assert not RuntimeConsumerLease.is_owned(run)
    return records, checkpoint


def test_fresh_source_cli_pause_resume_status_audit_workflow(tmp_path):
    """Every control is a new process; installed-package CLI remains CP6 scope."""
    from quant_futures.paper_runtime import (Lifecycle, LifecycleState,
                                               RuntimeConsumerLease)

    config_path, replay = _runtime_files(tmp_path)
    config_path.write_text(config_path.read_text().replace(
        "output_directory: .", f"output_directory: {tmp_path}"), encoding="utf-8")
    repository, env = _source_cli_env()
    owner = subprocess.Popen(
        [sys.executable, "-m", "quant_futures.product.cli", "paper", "start",
         "--config", str(config_path), "--replay", str(replay), "--pace", "5s"],
        cwd=repository, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert owner.stdout is not None
    run = Path(owner.stdout.readline().strip().split(": ", 1)[1])

    status = _fresh_cli("paper", "status", str(run))
    assert status.returncode == 0
    assert json.loads(status.stdout)["consumer_owner"] == "live"
    pause = _fresh_cli("paper", "pause", str(run))
    assert pause.returncode == 0
    owner.communicate(timeout=15)
    assert owner.returncode == 0
    assert Lifecycle(run).current().state is LifecycleState.PAUSED
    assert not RuntimeConsumerLease.is_owned(run)

    paused = json.loads(_fresh_cli("paper", "status", str(run)).stdout)
    assert paused["lifecycle"] == "PAUSED"
    assert paused["consumer_owner"] == "relinquished"
    resume = _fresh_cli("paper", "resume", str(run), timeout=30)
    assert resume.returncode == 0, resume.stderr
    assert Lifecycle(run).current().state is LifecycleState.COMPLETED
    assert _fresh_cli("paper", "audit", str(run)).returncode == 0
    assert list(run.glob(".checkpoint.json.*.tmp")) == []
    assert not RuntimeConsumerLease.is_owned(run)


def test_fresh_source_cli_paused_stop_and_repeated_stop(tmp_path):
    """A relinquished PAUSED run accepts stop idempotently in fresh processes."""
    from quant_futures.paper_runtime import Lifecycle, LifecycleState, RuntimeConsumerLease

    config_path, replay = _runtime_files(tmp_path)
    config_path.write_text(config_path.read_text().replace(
        "output_directory: .", f"output_directory: {tmp_path}"), encoding="utf-8")
    repository, env = _source_cli_env()
    owner = subprocess.Popen(
        [sys.executable, "-m", "quant_futures.product.cli", "paper", "start",
         "--config", str(config_path), "--replay", str(replay), "--pace", "5s"],
        cwd=repository, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    assert owner.stdout is not None
    run = Path(owner.stdout.readline().strip().split(": ", 1)[1])
    assert _fresh_cli("paper", "pause", str(run)).returncode == 0
    owner.communicate(timeout=15)
    assert owner.returncode == 0
    assert Lifecycle(run).current().state is LifecycleState.PAUSED
    assert not RuntimeConsumerLease.is_owned(run)

    product_before = {name: (run / name).read_bytes()
                      for name in ("transitions.journal", "checkpoint.json")}
    first = _fresh_cli("paper", "stop", str(run))
    assert first.returncode == 0, first.stderr
    assert Lifecycle(run).current().state is LifecycleState.COMPLETED
    assert {name: (run / name).read_bytes() for name in product_before} == product_before
    lifecycle_after_first = (run / "lifecycle.jsonl").read_bytes()

    second = _fresh_cli("paper", "stop", str(run))
    assert second.returncode == 0, second.stderr
    assert (run / "lifecycle.jsonl").read_bytes() == lifecycle_after_first
    assert {name: (run / name).read_bytes() for name in product_before} == product_before
    status = json.loads(_fresh_cli("paper", "status", str(run)).stdout)
    assert status["lifecycle"] == "COMPLETED"
    assert status["consumer_owner"] == "relinquished"
    assert _fresh_cli("paper", "audit", str(run)).returncode == 0
    assert list(run.glob(".checkpoint.json.*.tmp")) == []
    assert not RuntimeConsumerLease.is_owned(run)

def test_fresh_source_cli_owner_death_status_recover_and_live_rejection(tmp_path):
    """A live owner rejects recovery; after death status stalls and recovery continues."""
    from quant_futures.paper_runtime import (Lifecycle, LifecycleState,
                                               RuntimeConsumerLease)

    config_path, replay = _runtime_files(tmp_path)
    config_path.write_text(config_path.read_text().replace(
        "output_directory: .", f"output_directory: {tmp_path}"), encoding="utf-8")
    repository, env = _source_cli_env()
    owner = subprocess.Popen(
        [sys.executable, "-m", "quant_futures.product.cli", "paper", "start",
         "--config", str(config_path), "--replay", str(replay), "--pace", "2s"],
        cwd=repository, env=env, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert owner.stdout is not None
    run = Path(owner.stdout.readline().strip().split(": ", 1)[1])
    lifecycle_before = (run / "lifecycle.jsonl").read_bytes()
    rejected = _fresh_cli("paper", "recover", str(run))
    assert rejected.returncode == 2
    assert (run / "lifecycle.jsonl").read_bytes() == lifecycle_before

    owner.kill(); owner.communicate(timeout=10)
    assert not RuntimeConsumerLease.is_owned(run)
    stalled = json.loads(_fresh_cli("paper", "status", str(run)).stdout)
    assert stalled["consumer_owner"] == "relinquished"
    assert stalled["stalled"] is True
    recovered = _fresh_cli("paper", "recover", str(run), timeout=30)
    assert recovered.returncode == 0, recovered.stderr
    assert Lifecycle(run).current().state is LifecycleState.COMPLETED
    assert _fresh_cli("paper", "audit", str(run)).returncode == 0
    assert list(run.glob(".checkpoint.json.*.tmp")) == []


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
@pytest.mark.parametrize("location", ["inside", "waiting"])
def test_source_cli_actual_signals_stop_at_committed_boundary(
    tmp_path, signum, location,
):
    """Exercise actual signals in a source-tree CLI process, not a handler call."""
    config_path, replay = _runtime_files(tmp_path)
    repository = Path(__file__).parents[2]
    run_id = "ab" * 16

    def launch(kind, root):
        reached, release = root / "reached", root / "release"
        root.mkdir()
        env = os.environ.copy()
        env.update({"PYTHONPATH": str(repository / "src"),
                    "QFS_RUNTIME_TEST_ROOT": str(root / "runs"),
                    "QFS_RUNTIME_TEST_RUN_ID": run_id})
        if location == "inside":
            env.update({"QFS_RUNTIME_BOUNDARY": "journal:strategy_committed",
                        "QFS_RUNTIME_BOUNDARY_REACHED": str(reached),
                        "QFS_RUNTIME_BOUNDARY_RELEASE": str(release)})
        process = subprocess.Popen(
            [sys.executable, "-m", "quant_futures.product.cli", "paper", "start",
             "--config", str(config_path), "--replay", str(replay), "--pace", "0.5s"],
            cwd=repository, env=env, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        assert process.stdout is not None
        run = Path(process.stdout.readline().strip().split(": ", 1)[1])
        deadline = time.monotonic() + 10
        marker = reached if location == "inside" else run / "checkpoint.json"
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        if kind == "signal":
            process.send_signal(signum)
            control_process = None
        else:
            control_process = subprocess.Popen(
                [sys.executable, "-m", "quant_futures.product.cli", "paper", "stop", str(run)],
                cwd=repository, env=env, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        if location == "inside":
            release.touch()
        stdout, stderr = process.communicate(timeout=15)
        assert process.returncode == 0, (stdout, stderr)
        if control_process is not None:
            output, error = control_process.communicate(timeout=15)
            assert control_process.returncode == 0, (output, error)
        return run

    signal_run = launch("signal", tmp_path / "signal")
    baseline_run = launch("control", tmp_path / "baseline")
    signal_records, signal_checkpoint = _assert_terminal_authority(signal_run)
    baseline_records, baseline_checkpoint = _assert_terminal_authority(baseline_run)
    assert (signal_run / "transitions.journal").read_bytes() == (
        baseline_run / "transitions.journal").read_bytes()
    assert signal_records == baseline_records
    assert _checkpoint_product_core(signal_checkpoint) == _checkpoint_product_core(
        baseline_checkpoint)
    signal_lifecycle = [record.state.value for record in
                        __import__("quant_futures.paper_runtime", fromlist=["Lifecycle"])
                        .Lifecycle(signal_run).records()]
    baseline_lifecycle = [record.state.value for record in
                          __import__("quant_futures.paper_runtime", fromlist=["Lifecycle"])
                          .Lifecycle(baseline_run).records()]
    assert signal_lifecycle == baseline_lifecycle

    # COMPLETED is terminal: public fresh-process continuation paths reject it
    # without changing any authority or projection bytes.
    protected = ("lifecycle.jsonl", "transitions.journal", "checkpoint.json", "status.json")
    before = {name: (signal_run / name).read_bytes() for name in protected}
    for command in ("resume", "recover"):
        rejected = _fresh_cli("paper", command, str(signal_run))
        assert rejected.returncode == 2
        assert {name: (signal_run / name).read_bytes() for name in protected} == before


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
