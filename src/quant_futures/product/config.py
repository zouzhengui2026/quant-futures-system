"""Strict product configuration loading and normalization."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from pathlib import Path
from typing import Any

class ConfigError(ValueError):
    """An actionable configuration error."""


@dataclass(frozen=True, slots=True)
class DataConfig:
    path: str
    source: str = "sample"
    symbol: str = "BTC-USDT-PERP"
    timeframe: str = "1h"
    start: str | None = None
    end: str | None = None
    schema: dict[str, str] = field(default_factory=lambda: {
        "timestamp": "timestamp", "open": "open", "high": "high",
        "low": "low", "close": "close", "volume": "volume"})


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    name: str = "moving_average_crossover"
    parameters: dict[str, Any] = field(default_factory=lambda: {"fast": 3, "slow": 5})


@dataclass(frozen=True, slots=True)
class CostConfig:
    commission_bps: float = 2.0
    slippage_bps: float = 1.0
    funding_rate: float = 0.0


@dataclass(frozen=True, slots=True)
class RiskConfig:
    max_position: float = 1.0
    max_drawdown: float = 0.5
    require_positive_equity: bool = True
    max_gross_notional: float = 1_000_000.0
    max_abs_net_notional: float = 1_000_000.0
    max_position_notional: float = 1_000_000.0
    max_concentration_ratio: float = 1.0
    max_gross_exposure_multiple: float = 100.0


@dataclass(frozen=True, slots=True)
class ProductConfig:
    mode: str
    data: DataConfig
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    costs: CostConfig = field(default_factory=CostConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    starting_equity: float = 10_000.0
    fill_timing: str = "next_open"
    output_directory: str = "runs"
    random_seed: int = 0

    def normalized(self) -> dict[str, Any]:
        return asdict(self)


def _utc(value: str | None, name: str) -> None:
    if value is None:
        return
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConfigError(f"data.{name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timezone.utc.utcoffset(parsed):
        raise ConfigError(f"data.{name} must be UTC timezone-aware")


def load_config(path: str | Path) -> ProductConfig:
    """Load YAML with paths resolved relative to the configuration file."""
    config_path = Path(path).expanduser().resolve()
    try:
        raw = _parse_yaml(config_path.read_text(encoding="utf-8"))
    except (OSError, ConfigError) as exc:
        raise ConfigError(f"cannot load config {config_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("configuration root must be a mapping")
    unknown = set(raw) - {"mode", "data", "strategy", "costs", "risk", "starting_equity",
                              "fill_timing", "output_directory", "random_seed"}
    if unknown:
        raise ConfigError(f"unknown configuration keys: {', '.join(sorted(unknown))}")
    try:
        if not isinstance(raw.get("data"), dict):
            raise ConfigError("data must be a mapping")
        for section in ("strategy", "costs", "risk"):
            if section in raw and not isinstance(raw[section], dict):
                raise ConfigError(f"{section} must be a mapping")
        data_raw = dict(raw["data"])
        candidate = Path(data_raw["path"]).expanduser()
        data_raw["path"] = str(candidate.resolve() if candidate.is_absolute()
                               else (config_path.parent / candidate).resolve())
        output = Path(raw.get("output_directory", "runs")).expanduser()
        output = output.resolve() if output.is_absolute() else (config_path.parent / output).resolve()
        cfg = ProductConfig(
            mode=str(raw["mode"]), data=DataConfig(**data_raw),
            strategy=StrategyConfig(**raw.get("strategy", {})),
            costs=CostConfig(**raw.get("costs", {})), risk=RiskConfig(**raw.get("risk", {})),
            starting_equity=float(raw.get("starting_equity", 10_000)),
            fill_timing=str(raw.get("fill_timing", "next_open")),
            output_directory=str(output), random_seed=int(raw.get("random_seed", 0)))
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigError(f"invalid configuration: {exc}") from exc
    if cfg.mode not in {"backtest", "paper"}:
        raise ConfigError("mode must be 'backtest' or 'paper'")
    if cfg.fill_timing not in {"next_open", "current_close"}:
        raise ConfigError("fill_timing must be next_open or current_close")
    numerics = {
        "starting_equity": cfg.starting_equity, "costs.commission_bps": cfg.costs.commission_bps,
        "costs.slippage_bps": cfg.costs.slippage_bps, "costs.funding_rate": cfg.costs.funding_rate,
        "risk.max_position": cfg.risk.max_position, "risk.max_drawdown": cfg.risk.max_drawdown,
        "risk.max_gross_notional": cfg.risk.max_gross_notional,
        "risk.max_abs_net_notional": cfg.risk.max_abs_net_notional,
        "risk.max_position_notional": cfg.risk.max_position_notional,
        "risk.max_concentration_ratio": cfg.risk.max_concentration_ratio,
        "risk.max_gross_exposure_multiple": cfg.risk.max_gross_exposure_multiple,
    }
    if any(not isinstance(value, Real) or isinstance(value, bool) or not isfinite(value)
           for value in numerics.values()):
        raise ConfigError("all numeric configuration values must be finite numbers (not bool)")
    raw_numeric = [raw.get("starting_equity", 10_000), raw.get("random_seed", 0)]
    raw_numeric += [raw.get("costs", {}).get(k, 0) for k in ("commission_bps", "slippage_bps", "funding_rate")]
    raw_numeric += [raw.get("risk", {}).get(k, 1) for k in (
        "max_position", "max_drawdown", "max_gross_notional", "max_abs_net_notional",
        "max_position_notional", "max_concentration_ratio", "max_gross_exposure_multiple")]
    if any(isinstance(value, bool) for value in raw_numeric):
        raise ConfigError("numeric configuration values must not be bool")
    if cfg.starting_equity <= 0 or cfg.costs.commission_bps < 0 or cfg.costs.slippage_bps < 0:
        raise ConfigError("equity must be positive and costs must be non-negative")
    if cfg.risk.max_position <= 0 or not 0 < cfg.risk.max_drawdown <= 1:
        raise ConfigError("risk limits are outside their valid range")
    if not isinstance(cfg.risk.require_positive_equity, bool):
        raise ConfigError("risk.require_positive_equity must be a bool")
    if (min(cfg.risk.max_gross_notional, cfg.risk.max_abs_net_notional,
            cfg.risk.max_position_notional, cfg.risk.max_gross_exposure_multiple) <= 0
            or not 0 < cfg.risk.max_concentration_ratio <= 1):
        raise ConfigError("portfolio risk limits are outside their valid range")
    _utc(cfg.data.start, "start"); _utc(cfg.data.end, "end")
    if cfg.data.start and cfg.data.end:
        start = datetime.fromisoformat(cfg.data.start.replace("Z", "+00:00"))
        end = datetime.fromisoformat(cfg.data.end.replace("Z", "+00:00"))
        if start > end: raise ConfigError("data.start must not be after data.end")
    return cfg


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if not value:
        return {}
    if value in {"null", "~"}:
        return None
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
        return value[1:-1]
    try:
        return float(value) if any(c in value for c in ".eE") else int(value)
    except ValueError:
        return value


def _parse_yaml(text: str) -> dict[str, Any]:
    """Parse the deliberately small, safe mapping-only product YAML dialect."""
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for number, original in enumerate(text.splitlines(), 1):
        if not original.strip() or original.lstrip().startswith("#"):
            continue
        indent = len(original) - len(original.lstrip(" "))
        if "\t" in original[:indent] or ":" not in original:
            raise ConfigError(f"invalid YAML mapping at line {number}")
        key, value = original.strip().split(":", 1)
        while stack[-1][0] >= indent:
            stack.pop()
        parent = stack[-1][1]
        parsed = _parse_scalar(value.split(" #", 1)[0])
        parent[key] = parsed
        if isinstance(parsed, dict):
            stack.append((indent, parsed))
    return root
