"""Acceptance for the declared wheel/console-script boundary.

The subprocesses deliberately run outside the checkout with PYTHONPATH removed;
this catches accidental success caused by pytest's source-tree import setting.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


def _run(command: list[str], *, cwd: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, env=env, text=True,
                          capture_output=True, timeout=60, check=False)


def test_built_wheel_console_script_runs_without_source_tree_imports(tmp_path: Path) -> None:
    repository = Path(__file__).resolve().parents[2]
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    environment["PIP_DISABLE_PIP_VERSION_CHECK"] = "1"
    wheelhouse = tmp_path / "wheelhouse"
    wheelhouse.mkdir()
    built = subprocess.run([sys.executable, "-m", "pip", "wheel", "--no-deps",
                            "--wheel-dir", str(wheelhouse), str(repository)],
                           cwd=tmp_path, env=environment, capture_output=True,
                           text=True, timeout=120, check=False)
    if built.returncode:
        pytest.skip(f"isolated build dependencies unavailable: {built.stderr[-300:]}")
    wheels = list(wheelhouse.glob("quant_futures_system-*.whl"))
    assert len(wheels) == 1
    venv = tmp_path / "venv"
    subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
    python = venv / "bin" / "python"
    subprocess.run([str(python), "-m", "pip", "install", "--no-deps", str(wheels[0])], cwd=tmp_path,
                   env=environment, check=True, capture_output=True, text=True, timeout=120)
    console = venv / "bin" / "quant-futures"

    imported = _run([str(python), "-c",
        "import json,quant_futures; print(json.dumps(quant_futures.__path__._path))"],
        cwd=tmp_path, env=environment)
    assert imported.returncode == 0, imported.stderr
    import_paths = json.loads(imported.stdout)
    assert import_paths and all(str(repository / "src") not in path for path in import_paths)

    runtime_root = tmp_path / "installed-runs"
    environment["QFS_RUNTIME_TEST_ROOT"] = str(runtime_root)
    environment["QFS_RUNTIME_TEST_RUN_ID"] = "6" * 32
    started = _run([str(console), "paper", "start", "--config",
                    str(repository / "examples/btc_ma_paper.yaml"), "--replay",
                    str(repository / "examples/data/btc_usdt_1h.csv"), "--pace", "0s"],
                   cwd=tmp_path, env=environment)
    assert started.returncode == 0, started.stderr
    run = runtime_root / ("6" * 32)
    assert run.is_dir()

    status = _run([str(console), "paper", "status", str(run)], cwd=tmp_path, env=environment)
    assert status.returncode == 0, status.stderr
    projection = json.loads(status.stdout)
    assert projection["lifecycle"] == "COMPLETED"
    assert projection["consumer_owner"] == "relinquished"
    assert projection["health"] == "healthy"
    audited = _run([str(console), "paper", "audit", str(run)], cwd=tmp_path, env=environment)
    assert audited.returncode == 0 and "exact match" in audited.stdout

    for terminal_command in ("resume", "recover"):
        rejected = _run([str(console), "paper", terminal_command, str(run)],
                        cwd=tmp_path, env=environment)
        assert rejected.returncode == 2
    assert not list(run.glob(".*.tmp"))
