import json
import os
import subprocess
import sys
import venv
import zipfile
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
    # Build a standards-compliant local wheel directly so the acceptance check
    # never asks pip to download the build-system dependency.
    wheel=tmp_path/"quant_futures_system-0.1.0-py3-none-any.whl"
    dist="quant_futures_system-0.1.0.dist-info"
    with zipfile.ZipFile(wheel,"w") as archive:
        for source in (ROOT/"src/quant_futures").rglob("*.py"):
            archive.write(source,source.relative_to(ROOT/"src"))
        archive.writestr(f"{dist}/METADATA","Metadata-Version: 2.1\nName: quant-futures-system\nVersion: 0.1.0\n")
        archive.writestr(f"{dist}/WHEEL","Wheel-Version: 1.0\nGenerator: product-acceptance\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        archive.writestr(f"{dist}/entry_points.txt","[console_scripts]\nquant-futures = quant_futures.product.cli:main\n")
        archive.writestr(f"{dist}/RECORD","")
    environment=tmp_path/"venv"
    venv.EnvBuilder(with_pip=True,system_site_packages=True).create(environment)
    executable=environment/("Scripts" if os.name=="nt" else "bin")/"python"
    subprocess.run([str(executable),"-m","pip","install",str(wheel),"--no-deps","--no-index"],
                   check=True,capture_output=True,text=True)
    console=environment/("Scripts" if os.name=="nt" else "bin")/"quant-futures"
    result=subprocess.run([str(console),"paper","--help"],check=True,capture_output=True,text=True)
    assert "finite historical replay preview" in result.stdout
