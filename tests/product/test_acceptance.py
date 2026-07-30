import json
import os
import subprocess
import sys
import venv
from pathlib import Path

from quant_futures.product.config import DataConfig, ProductConfig, StrategyConfig
from quant_futures.product.orchestrator import completed_run_audit, run

ROOT=Path(__file__).parents[2]
REQUIRED={"manifest.json","config.resolved.yaml","summary.json","equity.csv","positions.csv",
          "trades.csv","risk_breaches.csv","events.jsonl","report.html","checkpoint.json","status.json"}
CORE=("manifest.json","summary.json","equity.csv","positions.csv","trades.csv","risk_breaches.csv","events.jsonl","report.html")


def _btc(root: Path):
    return ProductConfig("backtest",DataConfig(str(ROOT/"examples/data/btc_usdt_1h.csv"),source="synthetic",
                         symbol="BTC-USDT-PERP",timeframe="1h"),
                         StrategyConfig("moving_average_crossover",{"fast":2,"slow":4}),
                         output_directory=str(root))


def test_btc_golden_e2e_and_byte_stable_core_artifacts(tmp_path: Path):
    first_id,first,summary=run(_btc(tmp_path/"one"))
    second_id,second,second_summary=run(_btc(tmp_path/"two"))
    assert REQUIRED == {item.name for item in first.iterdir()}
    assert completed_run_audit(first) and completed_run_audit(second)
    manifest=json.loads((first/"manifest.json").read_text())
    checkpoint=json.loads((first/"checkpoint.json").read_text())
    final=checkpoint["final_record"]
    assert manifest["run_id"]==first_id and manifest["record_count"]==12
    assert summary==second_summary and summary["trade_count"]==2
    assert summary["final_equity"]==9431.829128000001
    assert final["quantity"]==1 and final["equity"]==9431.829128000001 and not final["risk_breach"]
    assert len((first/"trades.csv").read_text().splitlines())==3
    # Output root is environment metadata in config/run identity. Core projections
    # deliberately contain neither path nor wall-clock metadata and are stable.
    for name in CORE:
        if name == "manifest.json":
            a=json.loads((first/name).read_text()); b=json.loads((second/name).read_text())
            a.pop("run_id"); b.pop("run_id")
            assert a==b
        else:
            assert (first/name).read_bytes()==(second/name).read_bytes(), name
    assert first_id != second_id


def test_installed_console_entry_point_offline_in_isolated_environment(tmp_path: Path):
    environment=tmp_path/"venv"
    venv.EnvBuilder(with_pip=True,system_site_packages=True).create(environment)
    executable=environment/("Scripts" if os.name=="nt" else "bin")/"python"
    # Seed the declared build backend from the interpreter/OS offline wheel
    # cache when the host environment does not already provide setuptools.
    candidates = (list((Path(sys.base_prefix)/"lib"/f"python{sys.version_info.major}.{sys.version_info.minor}"/
                        "test"/"wheeldata").glob("setuptools-*.whl"))
                  + list(Path("/usr/share/python-wheels").glob("setuptools-*.whl")))
    if candidates:
        subprocess.run([str(executable),"-m","pip","install","--no-index",str(candidates[0])],
                       check=True,capture_output=True,text=True)
    subprocess.run([str(executable),"-m","pip","install","--no-build-isolation","--no-deps",str(ROOT)],
                   check=True,capture_output=True,text=True)
    console=environment/("Scripts" if os.name=="nt" else "bin")/"quant-futures"
    installed_config=tmp_path/"installed.yaml"
    installed_config.write_text((ROOT/"examples/btc_ma.yaml").read_text()
        .replace("path: data/btc_usdt_1h.csv",f"path: {ROOT/'examples/data/btc_usdt_1h.csv'}")
        .replace("output_directory: ../runs/backtest",f"output_directory: {tmp_path/'runs'}"))
    subprocess.run([str(console),"validate-config","--config",str(installed_config)],
                   check=True,capture_output=True,text=True)
    result=subprocess.run([str(console),"backtest","--config",str(installed_config)],
                          check=True,capture_output=True,text=True,cwd=tmp_path)
    assert "final_equity:" in result.stdout
