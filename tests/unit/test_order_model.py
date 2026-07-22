from datetime import datetime, timezone

import pytest

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.domain.order import Order, OrderSide, OrderStatus


def test_order_defaults_to_created_status() -> None:
    order = Order(
        order_id="order-1",
        symbol="BTCUSDT",
        side=OrderSide.BUY,
        quantity=0.25,
        price=60_000.0,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert order.status is OrderStatus.CREATED
    assert order.side is OrderSide.BUY


def test_order_rejects_non_positive_quantity() -> None:
    with pytest.raises(DomainValidationError, match="quantity"):
        Order("order-1", "BTCUSDT", OrderSide.SELL, 0.0, None)
