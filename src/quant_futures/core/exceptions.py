"""Exception hierarchy for the runtime foundation."""


class QuantFuturesError(Exception):
    """Base exception for expected quant-futures runtime failures."""


class DomainValidationError(QuantFuturesError, ValueError):
    """Raised when a domain object is created with invalid data."""


class EventBusError(QuantFuturesError):
    """Raised for invalid event bus operations."""


class OrderLifecycleError(QuantFuturesError):
    """Raised when a paper order lifecycle operation is invalid."""


class PortfolioLedgerError(QuantFuturesError):
    """Raised when a portfolio ledger operation is invalid."""


class AccountValuationError(QuantFuturesError):
    """Raised when account valuation cannot be completed safely."""


class PortfolioRiskError(QuantFuturesError):
    """Raised when portfolio risk cannot be evaluated safely."""


class MarketDataError(QuantFuturesError):
    """Raised when market data cannot be normalized or safely collected."""
