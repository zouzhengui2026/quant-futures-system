"""Installed-wheel acceptance for the complete Paper operator boundary.

Every consumer is the wheel's console script, runs outside the checkout, and
inherits an environment with ``PYTHONPATH`` removed.  The fixture also proves
the interpreter and entry point provenance once, so process creation is safe
by construction rather than relying on each test remembering to sanitize it.
"""
from __future__ import annotations

import json
import os
import shutil
import signal
import struct
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(frozen=True)
class InstalledCLI:
    console: Path
    python: Path
    cwd: Path
    env: dict[str, str]
    repository: Path

    def run(self, *arguments: str, timeout: float = 60) -> subprocess.CompletedProcess[str]:
        return subprocess.run([str(self.console), *arguments], cwd=self.cwd, env=self.env,
                              text=True, capture_output=True, timeout=timeout, check=False)

    def popen(self, *arguments: str, env: dict[str, str] | None = None) -> subprocess.Popen[str]:
        return subprocess.Popen([str(self.console), *arguments], cwd=self.cwd,
                                env=env or self.env, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)

    def recover_process(self, run: Path, *, boundary: str | None = None) -> subprocess.Popen[str]:
        env = self.env.copy()
        if boundary is not None:
            reached = run.parent / f"{boundary}.reached"
            release = run.parent / f"{boundary}.release"
            env.update({"QFS_RUNTIME_BOUNDARY": boundary,
                        "QFS_RUNTIME_BOUNDARY_REACHED": str(reached),
                        "QFS_RUNTIME_BOUNDARY_RELEASE": str(release)})
        return self.popen("paper", "recover", str(run), env=env)

    def inspect(self, run: Path) -> dict[str, object]:
        """Read authority with the installed package, never the checkout."""
        code = (
            "import json,sys; from pathlib import Path; "
            "from quant_futures.paper_runtime import CheckpointStore,Lifecycle,TransitionJournal; "
            "p=Path(sys.argv[1]); rs=TransitionJournal(p).records(); "
            "print(json.dumps({'checkpoint':CheckpointStore(p).read(),"
            "'lifecycle':[r.state.value for r in Lifecycle(p).records()],"
            "'records':[r.as_dict() for r in rs]},sort_keys=True))"
        )
        result = subprocess.run([str(self.python), "-c", code, str(run)], cwd=self.cwd,
                                env=self.env, text=True, capture_output=True, check=False)
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)


@pytest.fixture(scope="session")
def installed_cli(tmp_path_factory: pytest.TempPathFactory) -> InstalledCLI:
    repository = Path(__file__).resolve().parents[2]
    root = tmp_path_factory.mktemp("installed-wheel")
    outside = root / "outside-checkout"
    outside.mkdir()
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.update({"PIP_DISABLE_PIP_VERSION_CHECK": "1", "PYTHONSAFEPATH": "1"})
    wheelhouse = root / "wheelhouse"
    wheelhouse.mkdir()
    built = subprocess.run(
        [sys.executable, "-m", "pip", "wheel", "--no-cache-dir", "--no-build-isolation", "--no-deps",
         "--wheel-dir", str(wheelhouse), str(repository)],
        cwd=outside, env=env, text=True, capture_output=True, timeout=120, check=False)
    assert built.returncode == 0, built.stderr
    wheels = list(wheelhouse.glob("quant_futures_system-*.whl"))
    assert len(wheels) == 1
    venv = root / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python, console = venv / "bin" / "python", venv / "bin" / "quant-futures"
    installed = subprocess.run([str(python), "-m", "pip", "install", "--no-deps",
                                str(wheels[0])], cwd=outside, env=env, text=True,
                               capture_output=True, timeout=120, check=False)
    assert installed.returncode == 0, installed.stderr
    provenance = subprocess.run(
        [str(python), "-c", "import importlib.metadata as m,quant_futures.product.cli as c;"
         "print(m.version('quant-futures-system'));print(c.__file__)"],
        cwd=outside, env=env, text=True, capture_output=True, check=False)
    assert provenance.returncode == 0, provenance.stderr
    assert provenance.stdout.splitlines()[0] == "0.1.0"
    assert str(venv) in provenance.stdout and str(repository) not in provenance.stdout
    assert console.resolve().is_relative_to(venv.resolve())
    return InstalledCLI(console, python, outside, env, repository)


def _files(root: Path, *, rows: int = 8, fill_timing: str = "next_open") -> tuple[Path, Path]:
    root.mkdir(parents=True, exist_ok=True)
    replay = root / "bars.csv"
    lines = ["timestamp,open,high,low,close,volume,funding_rate"]
    closes = [100, 103, 106, 101, 96, 99, 105, 98]
    for index in range(rows):
        close = closes[index % len(closes)]
        lines.append(f"2025-01-{1 + index // 24:02d}T{index % 24:02d}:00:00Z,{close-1},{close+2},{close-2},{close},10,{0.0001 if index % 3 == 2 else 0}")
    replay.write_text("\n".join(lines) + "\n", encoding="utf-8")
    config = root / "paper.yml"
    config.write_text(
        "mode: paper\ndata:\n  path: bars.csv\n  source: installed-acceptance\n"
        "  symbol: BTC\n  timeframe: 1h\nstrategy:\n  name: moving_average_crossover\n"
        "  parameters:\n    fast: 2\n    slow: 3\nstarting_equity: 10000\n"
        f"fill_timing: {fill_timing}\ncosts:\n  commission_bps: 2\n  slippage_bps: 1\n"
        "risk:\n  max_position: 1\n  max_drawdown: 0.5\n"
        f"output_directory: {root}\n", encoding="utf-8")
    return config, replay


def _launch(cli: InstalledCLI, root: Path, run_id: str, *, pace: str = "1s",
            extra: dict[str, str] | None = None,
            files: tuple[Path, Path] | None = None) -> tuple[subprocess.Popen[str], Path]:
    config, replay = files or _files(root)
    env = cli.env | {"QFS_RUNTIME_TEST_ROOT": str(root / "runs"),
                     "QFS_RUNTIME_TEST_RUN_ID": run_id}
    if extra:
        env.update(extra)
    owner = cli.popen("paper", "start", "--config", str(config), "--replay", str(replay),
                      "--pace", pace, env=env)
    assert owner.stdout is not None
    line = owner.stdout.readline().strip()
    assert line.startswith("run_directory: "), line
    return owner, Path(line.split(": ", 1)[1])


def _wait(path: Path, timeout: float = 15) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert path.exists(), path


def _eventually(cli: InstalledCLI, *arguments: str) -> subprocess.CompletedProcess[str]:
    """Retry a nonblocking control until the owner reaches its next boundary."""
    deadline = time.monotonic() + 15
    while True:
        result = cli.run(*arguments)
        if result.returncode == 0:
            return result
        assert (result.returncode == 2 and result.stderr.startswith("error: run directory is")
                and ("busy" in result.stderr or "controlled" in result.stderr))
        assert time.monotonic() < deadline, result.stderr
        time.sleep(0.02)


def _assert_clean(cli: InstalledCLI, run: Path, lifecycle: str = "COMPLETED") -> None:
    status = cli.run("paper", "status", str(run))
    assert status.returncode == 0 and status.stderr == ""
    projection = json.loads(status.stdout)
    assert projection["lifecycle"] == lifecycle
    assert projection["consumer_owner"] == "relinquished"
    audit = cli.run("paper", "audit", str(run))
    assert audit.returncode == 0 and audit.stdout == "paper runtime audit: exact match\n"
    assert not list(run.glob(".*.tmp"))


def _product_core(document: dict[str, object]) -> dict[str, object]:
    """Remove only separately validated control-plane recovery metadata."""
    return {key: value for key, value in document.items()
            if key not in {"lifecycle", "recovery_counter", "recovery_digest"}}


def _assert_product_equal(cli: InstalledCLI, expected: Path, actual: Path) -> None:
    left, right = cli.inspect(expected), cli.inspect(actual)
    assert (expected / "transitions.journal").read_bytes() == (
        actual / "transitions.journal").read_bytes()
    assert left["records"] == right["records"]
    assert _product_core(left["checkpoint"]) == _product_core(right["checkpoint"])
    records = right["records"]
    for field, stage in (("journal_event_id", None),
                         ("product_transition_id", "transition_started")):
        values = [record[field] for record in records
                  if stage is None or record["stage"] == stage]
        assert len(values) == len(set(values))
    for field in ("order_id", "fill_id"):
        values = [record["payload"][field] for record in records
                  if field in record["payload"]]
        # One deterministic ID legitimately appears in multiple causal stages;
        # its payload must always describe the same authority object.
        by_id: dict[str, set[str]] = {}
        for record in records:
            if field in record["payload"]:
                by_id.setdefault(record["payload"][field], set()).add(
                    json.dumps(record["payload"], sort_keys=True))
        assert values and all(by_id.values())


def test_installed_operator_pause_resume_and_paused_stop(installed_cli: InstalledCLI,
                                                         tmp_path: Path) -> None:
    owner, run = _launch(installed_cli, tmp_path / "resume", "1" * 32)
    status = installed_cli.run("paper", "status", str(run))
    assert status.returncode == 0 and json.loads(status.stdout)["consumer_owner"] == "live"
    # A live recovery/second consumer is rejected before authority mutation.
    before = (run / "lifecycle.jsonl").read_bytes()
    rejected = installed_cli.run("paper", "recover", str(run))
    assert rejected.returncode == 2 and rejected.stdout == "" and rejected.stderr.startswith("error: ")
    assert (run / "lifecycle.jsonl").read_bytes() == before
    _eventually(installed_cli, "paper", "pause", str(run))
    output, error = owner.communicate(timeout=20)
    assert owner.returncode == 0, (output, error)
    assert installed_cli.run("paper", "resume", str(run), timeout=30).returncode == 0
    _assert_clean(installed_cli, run)

    owner, paused = _launch(installed_cli, tmp_path / "stop", "2" * 32)
    _eventually(installed_cli, "paper", "pause", str(paused))
    output, error = owner.communicate(timeout=20)
    assert owner.returncode == 0, (output, error)
    product = {name: (paused / name).read_bytes()
               for name in ("transitions.journal", "checkpoint.json")}
    assert installed_cli.run("paper", "stop", str(paused)).returncode == 0
    lifecycle = (paused / "lifecycle.jsonl").read_bytes()
    assert installed_cli.run("paper", "stop", str(paused)).returncode == 0
    assert (paused / "lifecycle.jsonl").read_bytes() == lifecycle
    assert {name: (paused / name).read_bytes() for name in product} == product
    _assert_clean(installed_cli, paused)


def test_installed_owner_death_stalls_then_recovers(installed_cli: InstalledCLI,
                                                    tmp_path: Path) -> None:
    owner, run = _launch(installed_cli, tmp_path, "3" * 32, pace="0.5s")
    _wait(run / "checkpoint.json")
    owner.kill()
    owner.communicate(timeout=10)
    stalled = installed_cli.run("paper", "status", str(run))
    assert stalled.returncode == 0
    projection = json.loads(stalled.stdout)
    assert projection["stalled"] is True and projection["consumer_owner"] == "relinquished"
    recovered = installed_cli.run("paper", "recover", str(run), timeout=30)
    assert recovered.returncode == 0, recovered.stderr
    _assert_clean(installed_cli, run)


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_installed_actual_signals_at_committed_boundaries(installed_cli: InstalledCLI,
                                                          tmp_path: Path,
                                                          signum: signal.Signals) -> None:
    root = tmp_path / str(signum)
    reached, release = root / "reached", root / "release"
    root.mkdir()
    owner, run = _launch(installed_cli, root, ("4" if signum == signal.SIGINT else "5") * 32,
                         extra={"QFS_RUNTIME_BOUNDARY": "journal:strategy_committed",
                                "QFS_RUNTIME_BOUNDARY_REACHED": str(reached),
                                "QFS_RUNTIME_BOUNDARY_RELEASE": str(release)})
    _wait(reached)
    owner.send_signal(signum)
    release.touch()
    stdout, stderr = owner.communicate(timeout=20)
    assert owner.returncode == 0, (stdout, stderr)
    _assert_clean(installed_cli, run)
    before = {name: (run / name).read_bytes() for name in
              ("lifecycle.jsonl", "transitions.journal", "checkpoint.json")}
    for command in ("resume", "recover"):
        result = installed_cli.run("paper", command, str(run))
        assert result.returncode == 2 and result.stdout == "" and result.stderr.startswith("error: ")
    assert {name: (run / name).read_bytes() for name in before} == before


@pytest.mark.parametrize(("fill_timing", "boundary"), [
    ("current_close", "durable:strategy_committed"),
    ("current_close", "durable:fill_committed"),
    ("next_open", "durable:order_submitted"),
    ("next_open", "durable:fill_prepared"),
    ("next_open", "journal_fsync_completed"),
    ("next_open", "checkpoint_atomic_replace_completed"),
])
def test_installed_crash_recovery_golden_product_authority(
    installed_cli: InstalledCLI, tmp_path: Path, fill_timing: str, boundary: str,
) -> None:
    """Installed Product, journal, and checkpoint cuts converge exactly."""
    run_id = "6" * 32
    files = _files(tmp_path / "inputs", fill_timing=fill_timing)
    baseline_owner, baseline = _launch(installed_cli, tmp_path / "baseline", run_id,
                                       pace="0s", files=files)
    baseline_owner.communicate(timeout=30)
    assert baseline_owner.returncode == 0

    root = tmp_path / "crashed"
    reached, release = root / "reached", root / "release"
    root.mkdir()
    owner, recovered = _launch(
        installed_cli, root, run_id, pace="0s", files=files,
        extra={"QFS_RUNTIME_BOUNDARY": boundary,
               "QFS_RUNTIME_BOUNDARY_REACHED": str(reached),
               "QFS_RUNTIME_BOUNDARY_RELEASE": str(release)})
    _wait(reached)
    owner.kill(); owner.communicate(timeout=10)
    first = installed_cli.run("paper", "recover", str(recovered), timeout=30)
    assert first.returncode == 0, first.stderr
    # A terminal completed run rejects another invocation deterministically;
    # recovery-of-recovery publication itself is covered by the physical suite.
    assert installed_cli.run("paper", "recover", str(recovered)).returncode == 2

    _assert_product_equal(installed_cli, baseline, recovered)
    _assert_clean(installed_cli, recovered)


def test_installed_recovery_of_recovery_has_deterministic_exit(installed_cli: InstalledCLI,
                                                               tmp_path: Path) -> None:
    files = _files(tmp_path / "inputs", fill_timing="current_close")
    root = tmp_path / "crashed"; root.mkdir()
    reached, release = root / "product.reached", root / "product.release"
    owner, run = _launch(installed_cli, root, "8" * 32, pace="0s", files=files,
                         extra={"QFS_RUNTIME_BOUNDARY": "durable:fill_committed",
                                "QFS_RUNTIME_BOUNDARY_REACHED": str(reached),
                                "QFS_RUNTIME_BOUNDARY_RELEASE": str(release)})
    _wait(reached); owner.kill(); owner.communicate(timeout=10)
    recovery_reached = run.parent / "recovery_started_durable.reached"
    first = installed_cli.recover_process(run, boundary="recovery_started_durable")
    _wait(recovery_reached); first.kill(); first.communicate(timeout=10)
    second = installed_cli.run("paper", "recover", str(run), timeout=30)
    assert second.returncode == 0, second.stderr
    records = [json.loads(line) for line in (run / "recovery-attempts.jsonl").read_text().splitlines()]
    assert [record["outcome"] for record in records] == ["started", "failed", "started", "recovered"]
    checkpoint = installed_cli.inspect(run)["checkpoint"]
    assert checkpoint["recovery_counter"] == 2
    assert checkpoint["recovery_digest"] == records[-1]["digest"]
    _assert_clean(installed_cli, run)


def test_installed_long_replay_bounded_checkpoint_and_audit_tampers(
    installed_cli: InstalledCLI, tmp_path: Path,
) -> None:
    config, replay = _files(tmp_path / "soak", rows=96)
    env = installed_cli.env | {"QFS_RUNTIME_TEST_ROOT": str(tmp_path / "soak-runs"),
                               "QFS_RUNTIME_TEST_RUN_ID": "7" * 32}
    result = subprocess.run([str(installed_cli.console), "paper", "start", "--config", str(config),
                             "--replay", str(replay), "--pace", "0s"], cwd=installed_cli.cwd,
                            env=env, text=True, capture_output=True, timeout=60)
    assert result.returncode == 0, result.stderr
    run = tmp_path / "soak-runs" / ("7" * 32)
    authority = installed_cli.inspect(run)
    checkpoint = authority["checkpoint"]
    assert checkpoint["cursor"] == 96
    assert len([r for r in authority["records"] if r["stage"] == "transition_committed"]) == 96
    # Bounded restoration state: checkpoint size does not scale with 96 input rows.
    assert (run / "checkpoint.json").stat().st_size < 20_000
    _assert_clean(installed_cli, run)

    # Every mutation is isolated from the clean authority and exercised through
    # the installed audit entry point.  Rewriting disposable status cannot bless it.
    cases = {
        "journal-truncated": lambda p: p.write_bytes(p.read_bytes()[:-1]),
        "checkpoint-corrupt": lambda p: p.write_text("{}\n", encoding="utf-8"),
        "checkpoint-missing": lambda p: p.unlink(),
        "lifecycle-corrupt": lambda p: p.write_text("{}\n", encoding="utf-8"),
        "status-stale": lambda p: p.write_text("{}\n", encoding="utf-8"),
        "runtime-mismatch": lambda p: p.write_text("{}\n", encoding="utf-8"),
        "recovery-invalid": lambda p: p.write_text("{}\n", encoding="utf-8"),
    }
    names = {"journal-truncated": "transitions.journal", "checkpoint-corrupt": "checkpoint.json",
             "checkpoint-missing": "checkpoint.json",
             "lifecycle-corrupt": "lifecycle.jsonl", "status-stale": "status.json",
             "runtime-mismatch": "runtime.json", "recovery-invalid": "recovery-attempts.jsonl"}
    for label, mutate in cases.items():
        copy = tmp_path / f"tamper-{label}"
        shutil.copytree(run, copy)
        mutate(copy / names[label])
        audited = installed_cli.run("paper", "audit", str(copy))
        assert audited.returncode == 3 and audited.stdout == "paper runtime audit: mismatch\n"
    artifact = tmp_path / "tamper-artifact"
    shutil.copytree(run, artifact)
    (artifact / ".checkpoint.json.forged.tmp").mkdir()
    assert installed_cli.run("paper", "audit", str(artifact)).returncode == 3


def test_installed_restart_restore_soak_matches_uninterrupted(installed_cli: InstalledCLI,
                                                              tmp_path: Path) -> None:
    """Repeated owners restore bounded state rather than replaying committed input."""
    files = _files(tmp_path / "inputs", rows=64, fill_timing="next_open")
    run_id = "9" * 32
    baseline_owner, baseline = _launch(installed_cli, tmp_path / "baseline", run_id,
                                       pace="0s", files=files)
    baseline_owner.communicate(timeout=40)
    assert baseline_owner.returncode == 0

    owner, restarted = _launch(installed_cli, tmp_path / "restarted", run_id,
                               pace="0.2s", files=files)
    checkpoint_sizes: list[tuple[int, int]] = []
    for delay in (0.7, 1.0):
        time.sleep(delay)
        _eventually(installed_cli, "paper", "pause", str(restarted))
        stdout, stderr = owner.communicate(timeout=20)
        assert owner.returncode == 0, (stdout, stderr)
        committed = int(installed_cli.inspect(restarted)["checkpoint"]["cursor"])
        checkpoint_sizes.append((committed, (restarted / "checkpoint.json").stat().st_size))
        owner = installed_cli.popen("paper", "resume", str(restarted))
    time.sleep(1.0)
    owner.kill(); owner.communicate(timeout=10)
    stalled = json.loads(installed_cli.run("paper", "status", str(restarted)).stdout)
    assert stalled["stalled"] is True
    assert installed_cli.run("paper", "recover", str(restarted), timeout=45).returncode == 0

    authority = installed_cli.inspect(restarted)
    committed = [record for record in authority["records"]
                 if record["stage"] == "transition_committed"]
    assert [record["input_cursor"] for record in committed] == list(range(1, 65))
    assert [record["sequence"] for record in authority["records"]] == list(
        range(1, len(authority["records"]) + 1))
    assert len({record["product_transition_id"] for record in committed}) == 64
    assert checkpoint_sizes[0][0] < checkpoint_sizes[1][0]
    assert abs(checkpoint_sizes[1][1] - checkpoint_sizes[0][1]) < 2_000
    assert (restarted / "checkpoint.json").stat().st_size < 20_000
    _assert_product_equal(installed_cli, baseline, restarted)
    _assert_clean(installed_cli, restarted)


def test_installed_audit_semantic_foreign_and_reordered_authority(
    installed_cli: InstalledCLI, tmp_path: Path,
) -> None:
    files = _files(tmp_path / "inputs", rows=12)
    owner, clean = _launch(installed_cli, tmp_path / "clean", "a" * 32,
                           pace="0s", files=files)
    owner.communicate(timeout=30); assert owner.returncode == 0
    owner, foreign = _launch(installed_cli, tmp_path / "foreign", "b" * 32,
                             pace="0s", files=files)
    owner.communicate(timeout=30); assert owner.returncode == 0

    def copied(label: str) -> Path:
        destination = tmp_path / label
        shutil.copytree(clean, destination)
        return destination

    content = copied("journal-content")
    journal = bytearray((content / "transitions.journal").read_bytes())
    journal[len(journal) // 2] ^= 1
    (content / "transitions.journal").write_bytes(journal)

    reordered = copied("journal-reordered")
    data = (reordered / "transitions.journal").read_bytes()
    frames, offset = [], 0
    header = struct.Struct(">4sI32s")
    footer_size = struct.calcsize(">4sI32s")
    while offset < len(data):
        _, length, _ = header.unpack_from(data, offset)
        end = offset + header.size + length + footer_size
        frames.append(data[offset:end]); offset = end
    frames[1], frames[2] = frames[2], frames[1]
    (reordered / "transitions.journal").write_bytes(b"".join(frames))

    stale = copied("checkpoint-stale")
    stale_doc = json.loads((stale / "checkpoint.json").read_text())
    stale_doc["journal"]["sequence"] -= 1
    (stale / "checkpoint.json").write_text(
        json.dumps(stale_doc, sort_keys=True, separators=(",", ":")) + "\n")
    # Rewrite the disposable projection to demonstrate that it cannot bless
    # semantically stale checkpoint authority.
    (stale / "status.json").write_text("{}\n")

    foreign_checkpoint = copied("checkpoint-foreign")
    shutil.copy2(foreign / "checkpoint.json", foreign_checkpoint / "checkpoint.json")
    (foreign_checkpoint / "status.json").write_text("{}\n")

    for directory in (content, reordered, stale, foreign_checkpoint):
        result = installed_cli.run("paper", "audit", str(directory))
        assert result.returncode == 3
        assert result.stdout == "paper runtime audit: mismatch\n"
