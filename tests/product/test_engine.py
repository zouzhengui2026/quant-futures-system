from datetime import datetime, timedelta, timezone
from quant_futures.product.config import CostConfig, DataConfig, ProductConfig, RiskConfig
from quant_futures.product.data import Bar
from quant_futures.product.engine import simulate
from quant_futures.product.strategy import FixedStrategy
def test_next_open_and_costs():
    t=datetime(2024,1,1,tzinfo=timezone.utc); bars=tuple(Bar(t+timedelta(hours=i),100,101,99,100,1) for i in range(3))
    records=simulate(ProductConfig("backtest",DataConfig("x")), bars, FixedStrategy(1))
    assert records[0].quantity==0 and records[1].quantity==1 and records[1].commission>0
    assert records[2].fill_quantity == 0 and records[-1].quantity == 1

def test_next_open_reversal_is_exact_and_final_intent_is_cancelled():
    t=datetime(2024,1,1,tzinfo=timezone.utc); bars=tuple(Bar(t+timedelta(hours=i),100,101,99,100,1) for i in range(3))
    class Reversal:
        name="reversal"; version="1"
        def target(self, context): return (1, -1, 0)[len(context.closes)-1]
    records=simulate(ProductConfig("backtest",DataConfig("x")), bars, Reversal())
    assert [r.fill_quantity for r in records] == [0, 1, -2]
    assert records[-1].quantity == -1 and records[-1].final_pending_cancelled

def test_account_and_risk_snapshots_include_commission_and_funding():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    bars=tuple(Bar(t+timedelta(hours=i),100,100,100,100,1, .01 if i == 2 else None) for i in range(3))
    config=ProductConfig("backtest",DataConfig("x"),costs=CostConfig(100,0,.99))
    records=simulate(config,bars,FixedStrategy(1))
    assert records[1].commission == 1
    assert records[1].funding == 0
    assert records[2].funding == -1
    assert records[2].account_snapshot.cash_flow == -2
    assert records[2].equity == 9998
    assert records[1].portfolio_risk_snapshot.account_snapshot is records[1].account_snapshot

def test_resolved_portfolio_limits_and_stage_journal_are_authoritative():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    bars=(Bar(t,100,100,100,100,1),)
    events=[]
    risk=RiskConfig(max_gross_notional=50,max_abs_net_notional=50,max_position_notional=50)
    records=simulate(ProductConfig("backtest",DataConfig("x"),risk=risk,fill_timing="current_close"),
                     bars,FixedStrategy(1),events.append)
    assert records[0].portfolio_risk_snapshot.limits.max_gross_notional == 50
    assert records[0].risk_breach
    assert [event["sequence"] for event in events] == list(range(1,len(events)+1))
    assert {"fill_prepared","fill_committed","portfolio_committed","account_committed","risk_committed"} <= {event["stage"] for event in events}


def test_first_valuation_cost_drawdown_matches_authoritative_risk_at_full_limit():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    config=ProductConfig("backtest",DataConfig("x"),costs=CostConfig(100,0,0),
                         risk=RiskConfig(max_drawdown=1),fill_timing="current_close")
    first=simulate(config,(Bar(t,100,100,100,100,1),),FixedStrategy(1))[0]
    assert first.equity == 9999
    assert first.drawdown == first.portfolio_risk_snapshot.drawdown_ratio == .0001

def test_funding_is_explicit_and_charges_pre_event_position_for_both_fill_conventions():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    base=(Bar(t,100,100,100,100,1,.25), Bar(t+timedelta(hours=1),100,100,100,100,1,None),
          Bar(t+timedelta(hours=2),100,100,100,100,1,.01))
    current=simulate(ProductConfig("backtest",DataConfig("x"),costs=CostConfig(0,0,.5),
                                   fill_timing="current_close"),base,FixedStrategy(1))
    next_open=simulate(ProductConfig("backtest",DataConfig("x"),costs=CostConfig(0,0,.5),
                                     fill_timing="next_open"),base,FixedStrategy(1))
    # Timestamp-zero entries are not charged; missing row values do not invoke
    # the configured legacy rate; the carried long pays at timestamp two.
    assert [r.funding for r in current] == [0, 0, -1]
    assert [r.funding for r in next_open] == [0, 0, -1]


def test_explicit_zero_funding_and_short_sign():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    bars=(Bar(t,100,100,100,100,1,None), Bar(t+timedelta(hours=1),100,100,100,100,1,0.0),
          Bar(t+timedelta(hours=2),100,100,100,100,1,.01))
    records=simulate(ProductConfig("backtest",DataConfig("x"),fill_timing="current_close"),
                     bars,FixedStrategy(-1))
    assert [r.funding for r in records] == [0, 0, 1]
