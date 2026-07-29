from datetime import datetime, timedelta, timezone
from quant_futures.product.config import DataConfig, ProductConfig
from quant_futures.product.data import Bar
from quant_futures.product.engine import simulate
from quant_futures.product.strategy import FixedStrategy
def test_next_open_and_costs():
    t=datetime(2024,1,1,tzinfo=timezone.utc); bars=tuple(Bar(t+timedelta(hours=i),100,101,99,100,1) for i in range(3))
    records=simulate(ProductConfig("backtest",DataConfig("x")), bars, FixedStrategy(1))
    assert records[0].quantity==0 and records[1].quantity==1 and records[1].commission>0
