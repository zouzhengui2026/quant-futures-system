"""Timestamp-aware analytics and deterministic, self-contained reporting."""
from __future__ import annotations

import html
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from statistics import pstdev

from .config import ProductConfig
from .engine import Record
from .runtime import atomic_write

_SECONDS_PER_YEAR = 365.2425 * 24 * 60 * 60


@dataclass
class _Lot:
    quantity: float
    price: float
    commission: float
    slippage: float
    funding: float = 0.0


def completed_trades(records: tuple[Record, ...]) -> list[dict]:
    """Build FIFO round trips from authoritative fills.

    Funding at a timestamp belongs to the lots carried into that timestamp.
    Entry costs are allocated pro-rata when a lot is partially closed.  Open
    lots at run end remain unrealized and are excluded from trade statistics.
    Slippage is already present in authoritative fill prices and is reported as
    an allocation rather than subtracted a second time.
    """
    lots: list[_Lot] = []
    trades: list[dict] = []
    for record in records:
        open_quantity = sum(abs(lot.quantity) for lot in lots)
        if record.funding and open_quantity:
            for lot in lots:
                lot.funding += record.funding * abs(lot.quantity) / open_quantity
        quantity = record.fill_quantity
        if not quantity or record.fill_price is None:
            continue
        remaining = quantity
        total = abs(quantity)
        while lots and remaining * lots[0].quantity < 0:
            lot = lots[0]
            closed = min(abs(remaining), abs(lot.quantity))
            fraction = closed / abs(lot.quantity)
            entry_commission = lot.commission * fraction
            entry_slippage = lot.slippage * fraction
            funding = lot.funding * fraction
            exit_commission = record.commission * closed / total
            exit_slippage = record.slippage * closed / total
            direction = 1.0 if lot.quantity > 0 else -1.0
            price_pnl = direction * closed * (record.fill_price - lot.price)
            net = price_pnl - entry_commission - exit_commission + funding
            trades.append({
                "exit_timestamp": record.timestamp,
                "quantity": closed,
                "side": "long" if direction > 0 else "short",
                "entry_price": lot.price,
                "exit_price": record.fill_price,
                "price_pnl": price_pnl,
                "commission": entry_commission + exit_commission,
                "slippage": entry_slippage + exit_slippage,
                "funding": funding,
                "net_pnl": net,
            })
            lot.quantity -= direction * closed
            lot.commission -= entry_commission
            lot.slippage -= entry_slippage
            lot.funding -= funding
            remaining += direction * closed
            if abs(lot.quantity) < 1e-12:
                lots.pop(0)
        if abs(remaining) > 1e-12:
            used = abs(remaining) / total
            lots.append(_Lot(remaining, record.fill_price,
                             record.commission * used, record.slippage * used))
    return trades


def _ratio(numerator: float, denominator: float) -> float | None:
    value = numerator / denominator if denominator else None
    return value if value is None or math.isfinite(value) else None


def analytics(config: ProductConfig, records: tuple[Record, ...]) -> dict:
    start = config.starting_equity
    final = records[-1].equity if records else start
    timestamps = [datetime.fromisoformat(r.timestamp.replace("Z", "+00:00")) for r in records]
    elapsed_seconds = ((timestamps[-1] - timestamps[0]).total_seconds()
                       if len(timestamps) > 1 else 0.0)
    elapsed_years = elapsed_seconds / _SECONDS_PER_YEAR
    returns = [records[i].equity / records[i - 1].equity - 1
               for i in range(1, len(records)) if records[i - 1].equity > 0]
    periods_per_year = ((len(records) - 1) / elapsed_years if elapsed_years > 0 else 0.0)
    deviation = pstdev(returns) if len(returns) > 1 else 0.0
    mean = sum(returns) / len(returns) if returns else 0.0
    downside_period = math.sqrt(sum(min(value, 0.0) ** 2 for value in returns) / len(returns)) if returns else 0.0
    annualized = ((final / start) ** (1 / elapsed_years) - 1
                  if elapsed_years > 0 and final > 0 and start > 0 else 0.0)
    scale = math.sqrt(periods_per_year) if periods_per_year > 0 else 0.0

    longest_bars = current_bars = 0
    longest_seconds = current_start = 0.0
    for index, record in enumerate(records):
        if record.drawdown > 0:
            current_bars += 1
            if current_bars == 1:
                current_start = timestamps[index].timestamp()
            longest_bars = max(longest_bars, current_bars)
            longest_seconds = max(longest_seconds, timestamps[index].timestamp() - current_start)
        else:
            current_bars = 0

    trades = completed_trades(records)
    wins = [trade["net_pnl"] for trade in trades if trade["net_pnl"] > 0]
    losses = [trade["net_pnl"] for trade in trades if trade["net_pnl"] < 0]
    gross_profit = sum(wins)
    gross_loss = sum(losses)
    average_win = sum(wins) / len(wins) if wins else 0.0
    average_loss = sum(losses) / len(losses) if losses else 0.0
    breach_runs = longest_breach = current_breach = 0
    for record in records:
        current_breach = current_breach + 1 if record.risk_breach else 0
        if current_breach == 1:
            breach_runs += 1
        longest_breach = max(longest_breach, current_breach)
    filled = [record for record in records if record.fill_quantity]
    result = {
        "starting_equity": start, "final_equity": final, "absolute_return": final - start,
        "total_return": final / start - 1, "elapsed_seconds": elapsed_seconds,
        "annualized_return": annualized,
        "maximum_drawdown": max((record.drawdown for record in records), default=0.0),
        "maximum_drawdown_duration_bars": longest_bars,
        "maximum_drawdown_duration_seconds": longest_seconds,
        "annualized_volatility": deviation * scale,
        "sharpe_ratio": _ratio(mean * scale, deviation),
        "downside_deviation": downside_period * scale,
        "sortino_ratio": _ratio(mean * periods_per_year, downside_period * scale),
        "trade_count": len(trades), "win_rate": _ratio(len(wins), len(trades)),
        "gross_profit": gross_profit, "gross_loss": gross_loss,
        "average_win": average_win, "average_loss": average_loss,
        "profit_factor": _ratio(gross_profit, abs(gross_loss)),
        "payoff_ratio": _ratio(average_win, abs(average_loss)),
        "open_position_at_end": records[-1].quantity if records else 0.0,
        "exposure_bars": sum(record.quantity != 0 for record in records),
        "average_absolute_exposure": (sum(abs(record.quantity * record.price) for record in records) / len(records)
                                      if records else 0.0),
        "turnover": sum(abs(record.fill_quantity * record.fill_price) for record in filled if record.fill_price is not None),
        "total_commissions": sum(record.commission for record in records),
        "slippage_cost": sum(record.slippage for record in records),
        "funding_cash_flow": sum(record.funding for record in records),
        "risk_breach_count": sum(record.risk_breach for record in records),
        "risk_breach_episodes": breach_runs, "maximum_risk_breach_duration_bars": longest_breach,
    }
    return result


def _csv(columns: tuple[str, ...], rows: list[dict]) -> str:
    def cell(value: object) -> str:
        text = "" if value is None else str(value)
        return '"' + text.replace('"', '""') + '"' if any(c in text for c in ',"\n') else text
    return ",".join(columns) + "\n" + "".join(",".join(cell(row.get(key)) for key in columns) + "\n" for row in rows)


def _points(values: list[float], width: int = 800, height: int = 180) -> str:
    if not values:
        return ""
    low, high = min(values), max(values)
    span = high - low or 1.0
    return " ".join(f"{index * width / max(1, len(values)-1):.2f},{height - (value-low)/span*(height-10):.2f}"
                    for index, value in enumerate(values))


def write_artifacts(path: Path, config: ProductConfig, manifest: dict, records: tuple[Record, ...],
                    events: tuple[dict, ...]) -> dict:
    projected: dict[int, Record] = {}
    for event in events:
        if event["stage"] in {"bar_committed", "bar_amended"}:
            projected[event["transition_id"]] = Record(**event["payload"]["record"])
    records = tuple(projected[key] for key in sorted(projected))
    trades = completed_trades(records)
    summary = analytics(config, records)
    atomic_write(path / "manifest.json", json.dumps(manifest, sort_keys=True, indent=2) + "\n")
    atomic_write(path / "config.resolved.yaml", json.dumps(config.normalized(), sort_keys=True, indent=2) + "\n")
    atomic_write(path / "summary.json", json.dumps(summary, sort_keys=True, indent=2, allow_nan=False) + "\n")
    rows = [{name: getattr(record, name) for name in ("timestamp", "equity", "drawdown", "quantity", "price",
                                                       "fill_quantity", "fill_price", "commission", "slippage",
                                                       "funding", "risk_breach")} for record in records]
    atomic_write(path / "equity.csv", _csv(("timestamp", "equity", "drawdown"), rows))
    atomic_write(path / "positions.csv", _csv(("timestamp", "quantity", "price"), rows))
    atomic_write(path / "trades.csv", _csv(("exit_timestamp", "quantity", "side", "entry_price", "exit_price",
                                             "price_pnl", "commission", "slippage", "funding", "net_pnl"), trades))
    atomic_write(path / "risk_breaches.csv", _csv(("timestamp", "risk_breach", "drawdown"),
                                                    [row for row in rows if row["risk_breach"]]))
    atomic_write(path / "events.jsonl", "".join(json.dumps(event, sort_keys=True) + "\n" for event in events))

    cumulative_cost, costs = 0.0, []
    for record in records:
        cumulative_cost += record.commission + record.slippage - record.funding
        costs.append(cumulative_cost)
    charts = (("Equity", [r.equity for r in records]), ("Drawdown", [r.drawdown for r in records]),
              ("Position / exposure", [r.quantity * r.price for r in records]), ("Cumulative costs", costs))
    chart_html = "".join(f"<section><h2>{html.escape(title)}</h2><svg role='img' aria-label='{html.escape(title)}' viewBox='0 0 800 190'><polyline fill='none' stroke='#276ef1' points='{_points(values)}'/></svg></section>"
                         for title, values in charts)
    coverage = (f"{records[0].timestamp} through {records[-1].timestamp}" if records else "no observations")
    risk = config.risk
    assumptions = [
        f"Data coverage: {coverage}; configured timeframe: {config.data.timeframe}.",
        f"Fill convention: {config.fill_timing}; next-open fills execute before that bar's decision.",
        "Funding convention: only explicit non-blank row funding values are events; the pre-event position is charged.",
        (f"Risk limits: max position {risk.max_position}, max drawdown {risk.max_drawdown}, gross notional "
         f"{risk.max_gross_notional}, net notional {risk.max_abs_net_notional}, position notional {risk.max_position_notional}."),
        "Limitations: finite historical single-symbol replay preview only; no restart continuation, live trading, liquidation, multi-symbol orchestration, Parquet, or production event sourcing. Open positions are not counted as completed trades.",
    ]
    table = "".join(f"<tr><th>{html.escape(str(key))}</th><td>{html.escape(str(value))}</td></tr>"
                    for key, value in summary.items())
    report = ("<!doctype html><html><head><meta charset='utf-8'><title>Quant Futures Report</title>"
              "<style>body{font:14px sans-serif;max-width:960px;margin:auto}svg{width:100%}table{border-collapse:collapse}td,th{padding:6px;border:1px solid #ccc;text-align:left}</style>"
              "</head><body><h1>Run report</h1>" + chart_html + "<h2>Summary</h2><table>" + table +
              "</table><h2>Conventions and limitations</h2><ul>" +
              "".join(f"<li>{html.escape(text)}</li>" for text in assumptions) + "</ul></body></html>\n")
    atomic_write(path / "report.html", report)
    return summary
