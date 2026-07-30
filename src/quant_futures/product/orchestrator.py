"""Finite backtest and historical replay-preview orchestration.

Completed-run audit is deliberately separate from interrupted execution.  Product
v0.1 does not support continuation or recovery of an interrupted run.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from dataclasses import replace
from pathlib import Path

from .config import CostConfig, DataConfig, ProductConfig, RiskConfig, StrategyConfig
from .data import load_bars
from .engine import record_dict, simulate
from .reporting import write_artifacts
from .runtime import atomic_write, create_run_directory, run_id
from .strategy import build_strategy

_ARTIFACT_NAMES = ("manifest.json", "config.resolved.yaml", "events.jsonl", "summary.json",
                   "equity.csv", "positions.csv", "trades.csv", "risk_breaches.csv", "report.html")
_CHECKPOINT_KEYS = {"schema_version", "lifecycle", "run_id", "last_transition", "last_sequence",
                    "record_count", "event_count", "final_record", "event_digest", "artifact_digests"}
_MANIFEST_KEYS = {"schema_version", "run_id", "mode", "data_fingerprint", "strategy", "record_count",
                  "event_count", "real_money_trading", "limitations"}
_EVENT_KEYS = {"sequence", "transition_id", "stage", "timestamp", "payload"}


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(config: ProductConfig, replay_path: str | None = None) -> tuple[str, Path, dict]:
    """Execute a finite run; ``paper --replay`` is only a historical preview."""
    effective = replace(config, data=replace(config.data, path=str(Path(replay_path).resolve()))) if replay_path else config
    bars, fingerprint = load_bars(effective.data.path, effective.data.schema, start=effective.data.start,
                                   end=effective.data.end, timeframe=effective.data.timeframe)
    strategy = build_strategy(effective.strategy.name, effective.strategy.parameters)
    identifier = run_id(effective.normalized(), fingerprint, strategy.version)
    directory = create_run_directory(effective.output_directory, identifier)
    events: list[dict] = []

    def commit_stage(event: dict) -> None:
        events.append(event)
        # The preview journal is persisted at each stage for diagnostic value,
        # but an interrupted run is unsupported and cannot pass completed audit.
        atomic_write(directory / "events.jsonl", "".join(json.dumps(item, sort_keys=True) + "\n" for item in events))

    try:
        records = simulate(effective, bars, strategy, commit_stage)
        manifest = {
            "schema_version": 1, "run_id": identifier, "mode": effective.mode,
            "data_fingerprint": fingerprint,
            "strategy": {"name": strategy.name, "version": strategy.version,
                         "parameters": effective.strategy.parameters},
            "record_count": len(records), "event_count": len(events), "real_money_trading": False,
            "limitations": ["finite historical replay only", "single symbol", "CSV only",
                            "no interrupted-run continuation", "no production event sourcing"],
        }
        summary = write_artifacts(directory, effective, manifest, records, tuple(events))
        state = {
            "schema_version": 1, "lifecycle": "completed", "run_id": identifier,
            "last_transition": len(records), "last_sequence": len(events),
            "record_count": len(records), "event_count": len(events),
            "final_record": record_dict(records[-1]) if records else None,
            "event_digest": _digest(directory / "events.jsonl"),
            "artifact_digests": {name: _digest(directory / name) for name in _ARTIFACT_NAMES},
        }
        atomic_write(directory / "checkpoint.json", json.dumps(state, sort_keys=True, indent=2) + "\n")
        atomic_write(directory / "status.json", json.dumps({
            "schema_version": 1, "lifecycle": "completed", "run_id": identifier,
            "counters": {"bars": len(records), "events": len(events),
                         "fills": sum(bool(record.fill_quantity) for record in records),
                         "completed_trades": summary["trade_count"],
                         "risk_breaches": summary["risk_breach_count"]},
        }, sort_keys=True, indent=2) + "\n")
        (directory / ".lock").unlink()
        return identifier, directory, summary
    except BaseException:
        # An incomplete Product v0.1 run is not restartable.  Remove it so a
        # deterministic rerun is possible and never describe it as recoverable.
        shutil.rmtree(directory, ignore_errors=True)
        raise


def _load_exact_json(path: Path, keys: set[str]) -> dict | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) and set(value) == keys else None


def completed_run_audit(directory: str | Path) -> bool:
    """Fail-closed validation and canonical reconstruction of a completed run."""
    directory = Path(directory)
    state = _load_exact_json(directory / "checkpoint.json", _CHECKPOINT_KEYS)
    manifest = _load_exact_json(directory / "manifest.json", _MANIFEST_KEYS)
    status = _load_exact_json(directory / "status.json", {"schema_version", "lifecycle", "run_id", "counters"})
    try:
        raw = json.loads((directory / "config.resolved.yaml").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not state or not manifest or not status or state["schema_version"] != 1 or manifest["schema_version"] != 1:
        return False
    if state["lifecycle"] != "completed" or status["lifecycle"] != "completed":
        return False
    if set(state["artifact_digests"]) != set(_ARTIFACT_NAMES):
        return False
    if any(not isinstance(value, str) or len(value) != 64 for value in state["artifact_digests"].values()):
        return False
    try:
        if set(raw) != {"mode", "data", "strategy", "costs", "risk", "starting_equity",
                       "fill_timing", "output_directory", "random_seed"}:
            return False
        config = ProductConfig(raw["mode"], DataConfig(**raw["data"]), StrategyConfig(**raw["strategy"]),
                               CostConfig(**raw["costs"]), RiskConfig(**raw["risk"]), raw["starting_equity"],
                               raw["fill_timing"], raw["output_directory"], raw["random_seed"])
        if raw != config.normalized():
            return False
        bars, fingerprint = load_bars(config.data.path, config.data.schema, start=config.data.start,
                                       end=config.data.end, timeframe=config.data.timeframe)
        strategy = build_strategy(config.strategy.name, config.strategy.parameters)
        expected_id = run_id(config.normalized(), fingerprint, strategy.version)
        payload = (directory / "events.jsonl").read_bytes()
        if not payload.endswith(b"\n"):
            return False
        events = [json.loads(line) for line in payload.splitlines()]
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        return False
    if any(not isinstance(event, dict) or set(event) != _EVENT_KEYS for event in events):
        return False
    if [event["sequence"] for event in events] != list(range(1, len(events) + 1)):
        return False
    reconstructed_events: list[dict] = []
    try:
        records = simulate(config, bars, strategy, reconstructed_events.append)
    except (TypeError, ValueError):
        return False
    final_record = record_dict(records[-1]) if records else None
    expected_manifest = {
        "schema_version": 1, "run_id": expected_id, "mode": config.mode,
        "data_fingerprint": fingerprint,
        "strategy": {"name": strategy.name, "version": strategy.version,
                     "parameters": config.strategy.parameters},
        "record_count": len(records), "event_count": len(reconstructed_events),
        "real_money_trading": False,
        "limitations": ["finite historical replay only", "single symbol", "CSV only",
                        "no interrupted-run continuation", "no production event sourcing"],
    }
    try:
        summary = json.loads((directory / "summary.json").read_text(encoding="utf-8"))
        if not isinstance(summary, dict) or not isinstance(summary.get("trade_count"), int):
            return False
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    expected_counters = {"bars": len(records), "events": len(events),
                         "fills": sum(bool(record.fill_quantity) for record in records),
                         "completed_trades": summary["trade_count"],
                         "risk_breaches": sum(record.risk_breach for record in records)}
    try:
        with tempfile.TemporaryDirectory() as temporary:
            projection = Path(temporary)
            write_artifacts(projection, config, expected_manifest, records, tuple(reconstructed_events))
            projections_match = all((directory / name).read_bytes() == (projection / name).read_bytes()
                                    for name in _ARTIFACT_NAMES)
    except (OSError, TypeError, ValueError):
        return False
    return (
        expected_id == directory.name == state["run_id"] == manifest["run_id"] == status["run_id"]
        and manifest == expected_manifest
        and manifest["record_count"] == state["record_count"] == state["last_transition"] == len(records)
        and manifest["event_count"] == state["event_count"] == state["last_sequence"] == len(events)
        and state["final_record"] == final_record and events == reconstructed_events
        and hashlib.sha256(payload).hexdigest() == state["event_digest"]
        and status["counters"] == expected_counters
        and projections_match
        and all((directory / name).is_file() and _digest(directory / name) == digest
                for name, digest in state["artifact_digests"].items())
    )


# Stable CLI-facing name.  Its contract is explicitly completed-run audit only.
audit = completed_run_audit
