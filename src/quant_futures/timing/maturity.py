"""Opportunity maturity evaluation.

This module evaluates whether observed market conditions are mature enough
for downstream decision layers. It does not create trading signals or orders.
"""

from dataclasses import dataclass

from .state import TimingState


@dataclass(frozen=True)
class TimingResult:
    state: TimingState
    confidence: float
    reason: str


class OpportunityMaturityEvaluator:
    """Evaluate opportunity lifecycle stage."""

    def evaluate(
        self,
        observation_confidence: float,
        trend_strength: float,
        crowding_risk: float,
    ) -> TimingResult:
        if observation_confidence < 0.5:
            return TimingResult(
                TimingState.EARLY,
                observation_confidence,
                "insufficient market understanding",
            )

        if crowding_risk > 0.8:
            return TimingResult(
                TimingState.CROWDED,
                crowding_risk,
                "market participation is crowded",
            )

        if trend_strength > 0.7:
            return TimingResult(
                TimingState.MATURE,
                trend_strength,
                "market conditions are mature",
            )

        return TimingResult(
            TimingState.CONFIRMED,
            observation_confidence,
            "conditions are developing",
        )
