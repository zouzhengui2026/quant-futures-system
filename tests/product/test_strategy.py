from datetime import datetime, timezone
from quant_futures.product.data import Bar
from quant_futures.product.strategy import MovingAverageCrossover, StrategyContext
def test_ma_uses_context_only():
    b=Bar(datetime.now(timezone.utc),1,1,1,1,1)
    assert MovingAverageCrossover(2,3).target(StrategyContext(b,(1,2,3),0)) == 1
