from pathlib import Path

from quant_futures.product.cli import main
from quant_futures.product.config import load_config
from quant_futures.product.runtime import run_id


def test_config_resolves_relative_paths_and_is_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text("mode: backtest\ndata:\n  path: bars.csv\n")
    config = load_config(path)
    assert config.data.path == str(tmp_path / "bars.csv")
    assert run_id(config.normalized(), "abc", "v1") == run_id(config.normalized(), "abc", "v1")
    assert main(["validate-config", "--config", str(path)]) == 0


def test_invalid_mode_has_structured_exit(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("mode: live\ndata:\n  path: bars.csv\n")
    assert main(["validate-config", "--config", str(path)]) == 2
