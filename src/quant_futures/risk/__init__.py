"""Public API for immutable risk review."""

from .engine import RISK_UPDATED, RiskEngine
from .models import RiskAssessment, RiskOutcome
from .policies import ThresholdRiskPolicy
from .protocols import RiskPolicy


def __getattr__(name: str):
    """Load account-aware APIs lazily to avoid the account/execution import cycle."""
    if name in {"PORTFOLIO_RISK_UPDATED", "PortfolioRiskEngine"}:
        from . import portfolio_engine
        return getattr(portfolio_engine, name)
    if name in {
        "PortfolioRiskLimits", "PortfolioRiskOutcome", "PortfolioRiskSnapshot",
        "PositionExposure", "RiskLimitBreach", "RiskLimitCode",
    }:
        from . import portfolio_models
        return getattr(portfolio_models, name)
    raise AttributeError(name)

__all__ = [
    "RISK_UPDATED",
    "RiskAssessment",
    "RiskEngine",
    "RiskOutcome",
    "RiskPolicy",
    "ThresholdRiskPolicy",
    "PORTFOLIO_RISK_UPDATED",
    "PortfolioRiskEngine",
    "PortfolioRiskLimits",
    "PortfolioRiskOutcome",
    "PortfolioRiskSnapshot",
    "PositionExposure",
    "RiskLimitBreach",
    "RiskLimitCode",
]
