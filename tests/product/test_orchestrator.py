from pathlib import Path
from quant_futures.product.config import DataConfig, ProductConfig, StrategyConfig
from quant_futures.product.orchestrator import audit, recover, run
def test_run_writes_report_and_audits(tmp_path: Path):
    data=tmp_path/"x.csv"; data.write_text("timestamp,open,high,low,close,volume\n2024-01-01T00:00:00Z,1,1,1,1,1\n2024-01-01T01:00:00Z,1,1,1,1,1\n")
    cfg=ProductConfig("paper",DataConfig(str(data)),StrategyConfig("flat",{}),output_directory=str(tmp_path/"runs"))
    _,directory,_=run(cfg)
    assert (directory/"report.html").exists() and audit(directory)
    assert recover(directory)["last_transition"] == 2

def test_paper_journal_contains_incremental_stage_envelopes(tmp_path: Path):
    data=tmp_path/"x.csv"; data.write_text("timestamp,open,high,low,close,volume\n2024-01-01T00:00:00Z,1,1,1,1,1\n")
    cfg=ProductConfig("paper",DataConfig(str(data)),StrategyConfig("flat",{}),output_directory=str(tmp_path/"runs"))
    _,directory,_=run(cfg)
    import json
    events=[json.loads(line) for line in (directory/"events.jsonl").read_text().splitlines()]
    assert [event["stage"] for event in events] == ["bar_started","account_committed","risk_committed","bar_committed"]
    assert json.loads((directory/"checkpoint.json").read_text())["last_sequence"] == 4
