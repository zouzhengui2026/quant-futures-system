import json
import shutil
from pathlib import Path

import pytest

from quant_futures.product.config import DataConfig, ProductConfig, StrategyConfig
from quant_futures.product.orchestrator import completed_run_audit, run


def config(tmp_path: Path, mode: str = "backtest") -> ProductConfig:
    data=tmp_path/"x.csv"
    data.write_text("timestamp,open,high,low,close,volume,funding_rate\n"
                    "2024-01-01T00:00:00Z,100,100,100,100,1,\n"
                    "2024-01-01T01:00:00Z,100,101,100,101,1,0\n")
    return ProductConfig(mode,DataConfig(str(data)),StrategyConfig("flat",{}),
                         output_directory=str(tmp_path/"runs"))


def test_run_writes_self_contained_report_and_completed_audit(tmp_path: Path):
    identifier,directory,_=run(config(tmp_path,"paper"))
    assert identifier == directory.name and completed_run_audit(directory)
    report=(directory/"report.html").read_text()
    assert report.count("<svg") == 4
    assert "finite historical single-symbol replay preview only" in report


@pytest.mark.parametrize("target,mutation", [
    ("checkpoint.json", lambda value: value.pop("artifact_digests")),
    ("checkpoint.json", lambda value: value["artifact_digests"].update({"extra": "0"*64})),
    ("checkpoint.json", lambda value: value.update({"run_id": "changed"})),
    ("manifest.json", lambda value: value.pop("record_count")),
    ("manifest.json", lambda value: value["strategy"].update({"version": "tampered"})),
])
def test_audit_rejects_deleted_extra_or_changed_schema_fields(tmp_path: Path, target, mutation):
    _,directory,_=run(config(tmp_path))
    value=json.loads((directory/target).read_text()); mutation(value)
    (directory/target).write_text(json.dumps(value,sort_keys=True,indent=2)+"\n")
    assert not completed_run_audit(directory)


@pytest.mark.parametrize("mutation", [
    lambda events: events.pop(),
    lambda events: events.append(events[-1]),
    lambda events: events.reverse(),
    lambda events: events[0].update({"stage":"changed"}),
])
def test_audit_rejects_truncated_duplicated_reordered_or_changed_events(tmp_path: Path, mutation):
    _,directory,_=run(config(tmp_path))
    path=directory/"events.jsonl"; events=[json.loads(line) for line in path.read_text().splitlines()]
    mutation(events); path.write_text("".join(json.dumps(x,sort_keys=True)+"\n" for x in events))
    assert not completed_run_audit(directory)


def test_audit_rejects_modified_or_missing_every_required_artifact(tmp_path: Path):
    _,original,_=run(config(tmp_path))
    for name in json.loads((original/"checkpoint.json").read_text())["artifact_digests"]:
        copy=tmp_path/f"copy-{name.replace('.', '-')}"; shutil.copytree(original,copy)
        if name == "report.html": (copy/name).unlink()
        else: (copy/name).write_bytes((copy/name).read_bytes()+b"x")
        assert not completed_run_audit(copy)


def test_replay_override_is_persisted_and_auditable(tmp_path: Path):
    cfg=config(tmp_path,"paper")
    override=tmp_path/"override.csv"; shutil.copy(cfg.data.path,override)
    _,directory,_=run(cfg,str(override))
    assert json.loads((directory/"config.resolved.yaml").read_text())["data"]["path"] == str(override.resolve())
    assert completed_run_audit(directory)


@pytest.mark.parametrize("target,replacement", [
    ("checkpoint.json", {"artifact_digests": list(("manifest.json", "config.resolved.yaml", "events.jsonl",
        "summary.json", "equity.csv", "positions.csv", "trades.csv", "risk_breaches.csv", "report.html"))}),
    ("checkpoint.json", {"artifact_digests": None}),
    ("manifest.json", {"strategy": "flat"}),
    ("status.json", {"counters": []}),
    ("status.json", {"schema_version": 2}),
])
def test_audit_is_total_for_malformed_nested_json(tmp_path: Path, target, replacement):
    _,directory,_=run(config(tmp_path))
    path=directory/target; value=json.loads(path.read_text()); value.update(replacement)
    path.write_text(json.dumps(value,sort_keys=True,indent=2)+"\n")
    assert completed_run_audit(directory) is False
