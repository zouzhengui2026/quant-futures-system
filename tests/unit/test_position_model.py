from datetime import datetime, timezone

import pytest

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.domain.position import Position, PositionSide, PositionStatus


def test_position_stores_open_long_state() -> None:
    position = Position(
        symbol="BTCUSDT",
        side=PositionSide.LONG,
        size=0.25,
        entry_price=60_000.0,
        liquidation_price=48_000.0,
        updated_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert position.status is PositionStatus.OPEN
    assert position.side is PositionSide.LONG


def test_open_position_rejects_zero_size() -> None:
    with pytest.raises(DomainValidationError, match="open position"):
        Position("BTCUSDT", PositionSide.SHORT, 0.0, 60_000.0)
