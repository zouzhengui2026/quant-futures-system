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
        [sys.executable, "-m", "pip", "wheel", "--no-build-isolation", "--no-deps",
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
        assert result.returncode == 2 and result.stderr.startswith("error: run directory is busy")
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


def test_installed_crash_recovery_golden_product_authority(installed_cli: InstalledCLI,
                                                           tmp_path: Path) -> None:
    """A real Product-stage crash converges to uninterrupted Product bytes."""
    run_id = "6" * 32
    files = _files(tmp_path / "inputs")
    baseline_owner, baseline = _launch(installed_cli, tmp_path / "baseline", run_id,
                                       pace="0s", files=files)
    baseline_owner.communicate(timeout=30)
    assert baseline_owner.returncode == 0

    root = tmp_path / "crashed"
    reached, release = root / "reached", root / "release"
    root.mkdir()
    owner, recovered = _launch(
        installed_cli, root, run_id, pace="0s", files=files,
        extra={"QFS_RUNTIME_BOUNDARY": "durable:fill_committed",
               "QFS_RUNTIME_BOUNDARY_REACHED": str(reached),
               "QFS_RUNTIME_BOUNDARY_RELEASE": str(release)})
    _wait(reached)
    owner.kill(); owner.communicate(timeout=10)
    first = installed_cli.run("paper", "recover", str(recovered), timeout=30)
    assert first.returncode == 0, first.stderr
    # A terminal completed run rejects another invocation deterministically;
    # recovery-of-recovery publication itself is covered by the physical suite.
    assert installed_cli.run("paper", "recover", str(recovered)).returncode == 2

    baseline_doc, recovered_doc = installed_cli.inspect(baseline), installed_cli.inspect(recovered)
    assert (baseline / "transitions.journal").read_bytes() == (
        recovered / "transitions.journal").read_bytes()
    assert baseline_doc["records"] == recovered_doc["records"]
    operational = {"lifecycle", "recovery_counter", "recovery_digest"}
    assert {k: v for k, v in baseline_doc["checkpoint"].items() if k not in operational} == {
        k: v for k, v in recovered_doc["checkpoint"].items() if k not in operational}
    records = recovered_doc["records"]
    for field, stage in (("journal_event_id", None), ("product_transition_id", "transition_started")):
        values = [record[field] for record in records if stage is None or record["stage"] == stage]
        assert len(values) == len(set(values))
    _assert_clean(installed_cli, recovered)


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
