"""Finite analytics and self-contained run artifact rendering."""
from __future__ import annotations
import csv, html, json, math
from pathlib import Path
from statistics import pstdev
from .config import ProductConfig
from .engine import Record, record_dict
from .runtime import atomic_write

def analytics(config: ProductConfig, records: tuple[Record,...]) -> dict:
    start=config.starting_equity; final=records[-1].equity if records else start
    returns=[records[i].equity/records[i-1].equity-1 for i in range(1,len(records)) if records[i-1].equity>0]
    trades=[r for r in records if r.fill_quantity]
    vol=pstdev(returns)*math.sqrt(8760) if len(returns)>1 else 0.0
    mean=sum(returns)/len(returns) if returns else 0.0
    downside=math.sqrt(sum(min(x,0)**2 for x in returns)/len(returns))*math.sqrt(8760) if returns else 0.0
    annualized=(final/start)**(8760/max(1,len(records)))-1 if final > 0 and records else 0.0
    drawdown_bars=sum(r.drawdown > 0 for r in records)
    wins=[r for r in returns if r>0]; losses=[-r for r in returns if r<0]
    return {"starting_equity":start,"final_equity":final,"absolute_return":final-start,
      "total_return":final/start-1,"maximum_drawdown":max((r.drawdown for r in records),default=0),
      "annualized_return":annualized,"drawdown_duration_bars":drawdown_bars,
      "annualized_volatility":vol,"sharpe_ratio":mean/pstdev(returns)*math.sqrt(8760) if len(returns)>1 and pstdev(returns)>0 else None,
      "downside_deviation":downside,"sortino_ratio":mean*8760/downside if downside else None,"trade_count":len(trades),
      "winning_periods":len(wins),"losing_periods":len(losses),
      "profit_factor":sum(wins)/sum(losses) if losses else None,
      "payoff_ratio":(sum(wins)/len(wins))/(sum(losses)/len(losses)) if wins and losses else None,
      "exposure_bars":sum(r.quantity != 0 for r in records),
      "turnover":sum(abs(r.fill_quantity*r.fill_price) for r in trades if r.fill_price),
      "total_commissions":sum(r.commission for r in records),"slippage_cost":sum(r.slippage for r in records),
      "funding_cash_flow":sum(r.funding for r in records),"risk_breach_count":sum(r.risk_breach for r in records)}

def write_artifacts(path: Path, config: ProductConfig, manifest: dict, records: tuple[Record,...]) -> dict:
    summary=analytics(config,records)
    atomic_write(path/"manifest.json",json.dumps(manifest,sort_keys=True,indent=2)+"\n")
    atomic_write(path/"config.resolved.yaml",json.dumps(config.normalized(),sort_keys=True,indent=2)+"\n")
    atomic_write(path/"summary.json",json.dumps(summary,sort_keys=True,indent=2,allow_nan=False)+"\n")
    fields=list(record_dict(records[0])) if records else []
    for filename, selected in (("equity.csv",("timestamp","equity","drawdown")),("positions.csv",("timestamp","quantity","price")),
      ("trades.csv",("timestamp","fill_quantity","fill_price","commission","slippage")),("risk_breaches.csv",("timestamp","risk_breach","drawdown"))):
        rows=[r for r in records if filename not in {"trades.csv","risk_breaches.csv"} or (r.fill_quantity if filename=="trades.csv" else r.risk_breach)]
        content=",".join(selected)+"\n"+"".join(",".join(str(getattr(r,k)) for k in selected)+"\n" for r in rows)
        atomic_write(path/filename,content)
    atomic_write(path/"events.jsonl","".join(json.dumps(record_dict(r),sort_keys=True)+"\n" for r in records))
    if records:
        low=min(r.equity for r in records); high=max(r.equity for r in records); span=max(high-low,1.0)
        points=" ".join(f"{i*800/max(1,len(records)-1):.1f},{200-(r.equity-low)/span*180:.1f}" for i,r in enumerate(records))
    else: points=""
    report=f"<!doctype html><meta charset=utf-8><title>Quant Futures Report</title><style>body{{font:14px sans-serif;max-width:960px;margin:auto}}table{{border-collapse:collapse}}td,th{{padding:6px;border:1px solid #ccc}}</style><h1>Run report</h1><svg viewBox='0 0 800 220'><polyline fill='none' stroke='#276ef1' points='{points}'/></svg><h2>Summary</h2><table>"+"".join(f"<tr><th>{html.escape(k)}</th><td>{v}</td></tr>" for k,v in summary.items())+f"</table><h2>Assumptions</h2><p>Fill timing: {config.fill_timing}. Fixed bps costs; sample UTC bar coverage. Single account, market simulation only; no leverage, liquidation, or real trading.</p>"
    atomic_write(path/"report.html",report)
    return summary
