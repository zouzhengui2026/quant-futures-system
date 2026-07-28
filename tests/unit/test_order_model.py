from dataclasses import FrozenInstanceError
from datetime import datetime, timezone, tzinfo

import pytest

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.domain.order import Order, OrderSide, OrderStatus

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def order(**changes: object) -> Order:
    values = dict(order_id="order-1", symbol="BTCUSDT", side=OrderSide.BUY,
                  quantity=0.25, price=None, status=OrderStatus.CREATED, created_at=NOW)
    values.update(changes)
    return Order(**values)  # type: ignore[arg-type]


def test_order_is_frozen_slotted_supports_sides_and_prices() -> None:
    market = order()
    limit = order(side=OrderSide.SELL, price=60_000.0)
    assert not hasattr(market, "__dict__")
    assert market.status is OrderStatus.CREATED and market.price is None
    assert limit.side is OrderSide.SELL and limit.price == 60_000.0
    with pytest.raises((FrozenInstanceError, AttributeError)):
        market.symbol = "ETHUSDT"  # type: ignore[misc]
    market.validate()


@pytest.mark.parametrize("field,value", [
    ("order_id", ""), ("order_id", 1), ("symbol", " "), ("symbol", None),
    ("side", "buy"), ("status", "created"),
])
def test_order_rejects_invalid_identity_enums(field: str, value: object) -> None:
    with pytest.raises(DomainValidationError):
        order(**{field: value})


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), float("-inf"), "1"])
def test_order_rejects_invalid_quantity(value: object) -> None:
    with pytest.raises(DomainValidationError, match="quantity"):
        order(quantity=value)


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf"), float("-inf"), "1"])
def test_order_rejects_invalid_price(value: object) -> None:
    with pytest.raises(DomainValidationError, match="price"):
        order(price=value)


class NoneOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> None:
        return None


class BrokenOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> None:
        raise RuntimeError("broken")


@pytest.mark.parametrize("value", [datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=NoneOffset()), datetime(2026, 1, 1, tzinfo=BrokenOffset()), "now"])
def test_order_requires_effectively_aware_datetime(value: object) -> None:
    with pytest.raises(DomainValidationError, match="timezone-aware"):
        order(created_at=value)


def test_order_revalidation_fails_closed_after_tampering() -> None:
    value = order()
    object.__setattr__(value, "quantity", True)
    with pytest.raises(DomainValidationError, match="quantity"):
        value.validate()
