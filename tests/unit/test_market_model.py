from datetime import datetime, timezone

import pytest

from quant_futures.core.exceptions import DomainValidationError
from quant_futures.domain.market import MarketSnapshot


def test_market_snapshot_stores_market_observation() -> None:
    snapshot = MarketSnapshot(
        symbol="BTCUSDT",
        price=60_000.0,
        volume=1_200.0,
        funding_rate=0.0001,
        open_interest=500_000.0,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert snapshot.symbol == "BTCUSDT"
    assert snapshot.price == 60_000.0


def test_market_snapshot_rejects_negative_volume() -> None:
    with pytest.raises(DomainValidationError, match="volume"):
        MarketSnapshot(
            symbol="BTCUSDT",
            price=60_000.0,
            volume=-1.0,
            funding_rate=0.0,
            open_interest=500_000.0,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
