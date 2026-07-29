"""Backtest and recoverable replay-paper orchestration."""
from __future__ import annotations
import hashlib, json
from pathlib import Path
from .config import ProductConfig
from .data import load_bars
from .engine import simulate, record_dict
from .reporting import write_artifacts
from .runtime import atomic_write, create_run_directory, run_id
from .strategy import build_strategy

def run(config: ProductConfig, replay_path: str|None=None) -> tuple[str,Path,dict]:
    path=replay_path or config.data.path
    bars,fingerprint=load_bars(path,config.data.schema)
    strategy=build_strategy(config.strategy.name,config.strategy.parameters)
    identifier=run_id(config.normalized(),fingerprint,strategy.version)
    directory=create_run_directory(config.output_directory,identifier)
    try:
        records=simulate(config,bars,strategy)
        manifest={"run_id":identifier,"mode":config.mode,"data_fingerprint":fingerprint,
          "strategy":{"name":strategy.name,"version":strategy.version},"record_count":len(records),
          "real_money_trading":False}
        summary=write_artifacts(directory,config,manifest,records)
        state={"lifecycle":"stopped","last_transition":len(records),"final_record":record_dict(records[-1]) if records else None,
               "event_digest":hashlib.sha256((directory/"events.jsonl").read_bytes()).hexdigest()}
        atomic_write(directory/"checkpoint.json",json.dumps(state,sort_keys=True,indent=2)+"\n")
        atomic_write(directory/"status.json",json.dumps({"lifecycle":"completed","counters":{"bars":len(records),"decisions":len(records),"fills":summary["trade_count"],"risk_breaches":summary["risk_breach_count"],"recovery_attempts":0}},sort_keys=True,indent=2)+"\n")
        (directory/".lock").unlink()
        return identifier,directory,summary
    except BaseException:
        (directory/".lock").unlink(missing_ok=True)
        raise

def audit(directory: str|Path) -> bool:
    directory=Path(directory); state=json.loads((directory/"checkpoint.json").read_text())
    payload=(directory/"events.jsonl").read_bytes()
    events=[json.loads(line) for line in payload.splitlines()]
    return (hashlib.sha256(payload).hexdigest()==state["event_digest"] and
            state["last_transition"]==len(events) and (not events or events[-1]==state["final_record"]))

def recover(directory: str|Path) -> dict:
    """Fail closed and return the latest valid, non-duplicated paper state."""
    directory=Path(directory)
    if (directory/".lock").exists(): raise RuntimeError("run directory has an active writer lock")
    if not audit(directory): raise RuntimeError("checkpoint or event journal corruption detected")
    return json.loads((directory/"checkpoint.json").read_text(encoding="utf-8"))
