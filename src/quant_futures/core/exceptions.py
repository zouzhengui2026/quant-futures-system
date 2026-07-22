"""Exception hierarchy for the runtime foundation."""


class QuantFuturesError(Exception):
    """Base exception for expected quant-futures runtime failures."""


class DomainValidationError(QuantFuturesError, ValueError):
    """Raised when a domain object is created with invalid data."""


class EventBusError(QuantFuturesError):
    """Raised for invalid event bus operations."""


class MarketDataError(QuantFuturesError):
    """Raised when market data cannot be normalized or safely collected."""
