"""Backtest and recoverable replay-paper orchestration."""
from __future__ import annotations
import hashlib, json, shutil
from pathlib import Path
from .config import CostConfig, DataConfig, ProductConfig, RiskConfig, StrategyConfig
from .data import load_bars
from .engine import simulate, record_dict
from .reporting import write_artifacts
from .runtime import atomic_write, create_run_directory, run_id
from .strategy import build_strategy

def run(config: ProductConfig, replay_path: str|None=None) -> tuple[str,Path,dict]:
    path=replay_path or config.data.path
    bars,fingerprint=load_bars(path,config.data.schema, start=config.data.start,
                               end=config.data.end, timeframe=config.data.timeframe)
    strategy=build_strategy(config.strategy.name,config.strategy.parameters)
    identifier=run_id(config.normalized(),fingerprint,strategy.version)
    expected=Path(config.output_directory)/identifier
    if expected.exists() and not (expected/".lock").exists():
        try: lifecycle=json.loads((expected/"status.json").read_text()).get("lifecycle")
        except (OSError, json.JSONDecodeError): lifecycle=None
        if lifecycle in {"failed", "recoverable"}:
            shutil.rmtree(expected)
    directory=create_run_directory(config.output_directory,identifier)
    events: list[dict] = []
    def commit_stage(event: dict) -> None:
        events.append(event)
        # Paper replay is an incremental lifecycle: every engine boundary is
        # durably journaled and every completed bar advances its checkpoint.
        atomic_write(directory/"events.jsonl", "".join(
            json.dumps(item,sort_keys=True)+"\n" for item in events))
        if config.mode == "paper" and event["stage"] == "bar_committed":
            atomic_write(directory/"checkpoint.json", json.dumps({
                "lifecycle":"running", "last_transition":event["transition_id"],
                "last_sequence":event["sequence"], "final_record":event["payload"]["record"],
            },sort_keys=True,indent=2)+"\n")
    try:
        records=simulate(config,bars,strategy,commit_stage)
        manifest={"run_id":identifier,"mode":config.mode,"data_fingerprint":fingerprint,
          "strategy":{"name":strategy.name,"version":strategy.version},"record_count":len(records),
          "real_money_trading":False}
        summary=write_artifacts(directory,config,manifest,records,tuple(events))
        artifact_names=("events.jsonl","summary.json","equity.csv","positions.csv","trades.csv","risk_breaches.csv","report.html")
        state={"lifecycle":"stopped","last_transition":len(records),"last_sequence":len(events),"final_record":record_dict(records[-1]) if records else None,
               "event_digest":hashlib.sha256((directory/"events.jsonl").read_bytes()).hexdigest(),
               "artifact_digests":{name:hashlib.sha256((directory/name).read_bytes()).hexdigest() for name in artifact_names}}
        atomic_write(directory/"checkpoint.json",json.dumps(state,sort_keys=True,indent=2)+"\n")
        atomic_write(directory/"status.json",json.dumps({"lifecycle":"completed","counters":{"bars":len(records),"decisions":len(records),"fills":summary["trade_count"],"risk_breaches":summary["risk_breach_count"],"recovery_attempts":0}},sort_keys=True,indent=2)+"\n")
        (directory/".lock").unlink()
        return identifier,directory,summary
    except BaseException as exc:
        try:
            atomic_write(directory/"status.json",json.dumps({"lifecycle":"recoverable",
              "error":type(exc).__name__,"last_sequence":len(events),
              "last_transition":events[-1]["transition_id"] if events else 0},sort_keys=True,indent=2)+"\n")
        except BaseException:
            # If even failed-state persistence is unavailable, leave no
            # collision that could permanently strand this deterministic run.
            shutil.rmtree(directory, ignore_errors=True)
        finally:
            (directory/".lock").unlink(missing_ok=True)
        raise

def audit(directory: str|Path) -> bool:
    """Authoritatively reconstruct the run and require an exact state match."""
    directory=Path(directory); state=json.loads((directory/"checkpoint.json").read_text())
    raw=json.loads((directory/"config.resolved.yaml").read_text())
    config=ProductConfig(raw["mode"],DataConfig(**raw["data"]),StrategyConfig(**raw["strategy"]),
                         CostConfig(**raw["costs"]),RiskConfig(**raw["risk"]),raw["starting_equity"],
                         raw["fill_timing"],raw["output_directory"],raw["random_seed"])
    bars,fingerprint=load_bars(config.data.path,config.data.schema,start=config.data.start,
                               end=config.data.end,timeframe=config.data.timeframe)
    manifest=json.loads((directory/"manifest.json").read_text())
    if fingerprint != manifest["data_fingerprint"]: return False
    reconstructed_events=[]
    reconstructed=simulate(config,bars,build_strategy(config.strategy.name,config.strategy.parameters),reconstructed_events.append)
    expected=[record_dict(record) for record in reconstructed]
    payload=(directory/"events.jsonl").read_bytes()
    try: events=[json.loads(line) for line in payload.splitlines()]
    except json.JSONDecodeError: return False
    digests=state.get("artifact_digests",{})
    return (events==reconstructed_events and hashlib.sha256(payload).hexdigest()==state["event_digest"] and
            state["last_transition"]==len(expected) and (not expected or expected[-1]==state["final_record"]) and
            all((directory/name).is_file() and hashlib.sha256((directory/name).read_bytes()).hexdigest()==digest
                for name,digest in digests.items()))

def recover(directory: str|Path) -> dict:
    """Fail closed and return the latest valid, non-duplicated paper state."""
    directory=Path(directory)
    if (directory/".lock").exists(): raise RuntimeError("run directory has an active writer lock")
    if not audit(directory): raise RuntimeError("checkpoint or event journal corruption detected")
    return json.loads((directory/"checkpoint.json").read_text(encoding="utf-8"))
