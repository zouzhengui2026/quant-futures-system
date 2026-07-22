"""Small, deterministic rules for classifying a market environment."""

from __future__ import annotations

from typing import Protocol

from quant_futures.observation.models import (
    LiquidityRegime,
    MarketObservation,
    TrendRegime,
    VolatilityRegime,
)

from .models import RuleEvaluation, RuleOutcome


def _require_observation(observation: MarketObservation) -> None:
    if not isinstance(observation, MarketObservation):
        raise TypeError("evaluate expects a MarketObservation")


class TimingRule(Protocol):
    """Protocol implemented by rules that inspect a market observation."""

    def evaluate(self, observation: MarketObservation) -> RuleEvaluation:
        """Evaluate one environmental condition."""


class TrendAlignmentRule:
    """Require a directional trend regime; no trade direction is inferred."""

    name = "trend_alignment"

    def evaluate(self, observation: MarketObservation) -> RuleEvaluation:
        _require_observation(observation)
        if observation.trend_regime in (TrendRegime.UPTREND, TrendRegime.DOWNTREND):
            return RuleEvaluation(self.name, RuleOutcome.PASS, "market has a directional trend regime")
        return RuleEvaluation(self.name, RuleOutcome.NEUTRAL, "market is range-bound; await directional alignment")


class VolatilityConditionRule:
    """Prefer normal volatility and reject a high-volatility environment."""

    name = "volatility_condition"

    def evaluate(self, observation: MarketObservation) -> RuleEvaluation:
        _require_observation(observation)
        if observation.volatility_regime is VolatilityRegime.NORMAL:
            return RuleEvaluation(self.name, RuleOutcome.PASS, "volatility is within the normal range")
        if observation.volatility_regime is VolatilityRegime.HIGH:
            return RuleEvaluation(self.name, RuleOutcome.FAIL, "volatility is elevated")
        return RuleEvaluation(self.name, RuleOutcome.NEUTRAL, "volatility is low; await more active conditions")


class LiquidityConditionRule:
    """Prefer liquid conditions and reject stressed market liquidity."""

    name = "liquidity_condition"

    def evaluate(self, observation: MarketObservation) -> RuleEvaluation:
        _require_observation(observation)
        if observation.liquidity_regime is LiquidityRegime.LIQUID:
            return RuleEvaluation(self.name, RuleOutcome.PASS, "market liquidity is sufficient")
        if observation.liquidity_regime is LiquidityRegime.STRESSED:
            return RuleEvaluation(self.name, RuleOutcome.FAIL, "market liquidity is stressed")
        return RuleEvaluation(self.name, RuleOutcome.NEUTRAL, "market liquidity is thin; await improvement")
