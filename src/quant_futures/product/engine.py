"""Deterministic single-account simulation transition sequence."""
from __future__ import annotations
from dataclasses import asdict, dataclass
from .config import ProductConfig
from .data import Bar
from .strategy import Strategy, StrategyContext

@dataclass(frozen=True, slots=True)
class Record:
    transition_id: int; timestamp: str; price: float; target: float; quantity: float
    fill_quantity: float; fill_price: float|None; commission: float; slippage: float
    funding: float; cash: float; equity: float; drawdown: float; risk_breach: bool

def simulate(config: ProductConfig, bars: tuple[Bar,...], strategy: Strategy) -> tuple[Record,...]:
    cash=config.starting_equity; position=0.0; closes=[]; peak=cash; records=[]; pending=None
    for index, bar in enumerate(bars):
        closes.append(bar.close)
        target=strategy.target(StrategyContext(bar, tuple(closes), position))*config.risk.max_position
        requested=target-position
        fill_qty = pending if config.fill_timing=="next_open" else requested
        pending = requested if config.fill_timing=="next_open" else None
        fill_price=None; commission=slippage=0.0
        if fill_qty:
            reference=bar.open if config.fill_timing=="next_open" else bar.close
            fill_price=reference*(1 + (1 if fill_qty>0 else -1)*config.costs.slippage_bps/10000)
            slippage=abs(fill_qty)*(abs(fill_price-reference)); commission=abs(fill_qty*fill_price)*config.costs.commission_bps/10000
            cash -= fill_qty*fill_price+commission; position += fill_qty
        funding=-position*bar.close*(bar.funding_rate or config.costs.funding_rate); cash += funding
        equity=cash+position*bar.close; peak=max(peak,equity); drawdown=(peak-equity)/peak if peak else 0
        breach=drawdown>config.risk.max_drawdown
        records.append(Record(index+1,bar.timestamp.isoformat().replace("+00:00","Z"),bar.close,target,position,
                              fill_qty or 0.0,fill_price,commission,slippage,funding,cash,equity,drawdown,breach))
    return tuple(records)

def record_dict(record: Record) -> dict: return asdict(record)
