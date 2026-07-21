from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class MarketObservation:
    """Aggregated market state.

    This object represents the system's observation of the market,
    not a trading decision.
    """

    symbol: str
    price: float
    trend_state: str
    volatility_state: str
    funding_state: str
    open_interest_state: str
    liquidity_state: str
    crowding_state: str
    timestamp: datetime
