from datetime import datetime, timedelta, timezone
from quant_futures.product.config import DataConfig, ProductConfig
from quant_futures.product.engine import Record
from quant_futures.product.reporting import analytics, completed_trades


def record(i, timestamp, equity, quantity=0, fill=0, price=100, commission=0, slippage=0, funding=0):
    return Record(i,timestamp.isoformat().replace('+00:00','Z'),price,quantity,quantity,fill,
                  price if fill else None,commission,slippage,funding,equity-quantity*price,equity,
                  max(0,(10000-equity)/10000),False)


def test_actual_elapsed_annualization_and_longest_continuous_drawdown():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    records=tuple(record(i+1,t+timedelta(days=i),e) for i,e in enumerate((10000,9000,10000,9500,9400)))
    result=analytics(ProductConfig('backtest',DataConfig('x',timeframe='1d')),records)
    assert result['elapsed_seconds'] == 4*86400
    assert result['maximum_drawdown_duration_bars'] == 2
    assert result['maximum_drawdown_duration_seconds'] == 2*86400


def test_short_interval_positive_returns_never_overflow():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    cfg=ProductConfig('backtest',DataConfig('x'))
    for interval in (timedelta(seconds=1),timedelta(minutes=1)):
        result=analytics(cfg,(record(1,t,10000),record(2,t+interval,10100)))
        assert result['annualized_return'] is None


def test_drawdown_duration_runs_from_peak_through_recovery_or_final_observation():
    t=datetime(2024,1,1,tzinfo=timezone.utc); cfg=ProductConfig('backtest',DataConfig('x'))
    recovered=tuple(record(i+1,t+timedelta(minutes=i),e) for i,e in enumerate((10000,9000,10000)))
    underwater=recovered[:2]
    assert analytics(cfg,recovered)['maximum_drawdown_duration_bars']==2
    assert analytics(cfg,recovered)['maximum_drawdown_duration_seconds']==120
    assert analytics(cfg,underwater)['maximum_drawdown_duration_bars']==1
    assert analytics(cfg,underwater)['maximum_drawdown_duration_seconds']==60


def test_completed_trade_net_pnl_allocates_commission_funding_and_excludes_open_lot():
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    records=(record(1,t,9999,1,1,100,1,2),record(2,t+timedelta(hours=1),10008,0,-1,110,1,2,-1),
             record(3,t+timedelta(hours=2),10007,1,1,100,1,2))
    trades=completed_trades(records); result=analytics(ProductConfig('backtest',DataConfig('x')),records)
    assert len(trades)==1 and trades[0]['net_pnl']==7 and trades[0]['slippage']==4
    assert result['trade_count']==1 and result['win_rate']==1
    assert result['gross_profit']==7 and result['gross_loss']==0
    assert result['profit_factor'] is None and result['open_position_at_end']==1


def test_empty_and_non_positive_equity_metrics_remain_finite_json_values():
    cfg=ProductConfig('backtest',DataConfig('x'))
    empty=analytics(cfg,())
    t=datetime(2024,1,1,tzinfo=timezone.utc)
    negative=analytics(cfg,(record(1,t,-1),record(2,t+timedelta(minutes=1),0)))
    import json
    json.dumps(empty,allow_nan=False); json.dumps(negative,allow_nan=False)
