"""Timing-engine orchestration for market-environment assessment."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.observation.models import MarketObservation

from .filters import TimingStatusFilter
from .models import RuleEvaluation, TimingAssessment
from .rules import LiquidityConditionRule, TimingRule, TrendAlignmentRule, VolatilityConditionRule

TIMING_CREATED = EventType.TIMING_CREATED


@dataclass(slots=True)
class TimingEngine:
    """Evaluate market conditions and publish assessments without creating signals."""

    event_bus: EventBus
    rules: tuple[TimingRule, ...] = field(
        default_factory=lambda: (TrendAlignmentRule(), VolatilityConditionRule(), LiquidityConditionRule())
    )
    status_filter: TimingStatusFilter = field(default_factory=TimingStatusFilter)
    clock: Callable[[], datetime] = field(default=lambda: datetime.now(timezone.utc))

    def assess(self, observation: MarketObservation) -> TimingAssessment:
        """Assess one observation and publish its timing assessment."""
        if not isinstance(observation, MarketObservation):
            raise TypeError("assess expects a MarketObservation")

        evaluations: tuple[RuleEvaluation, ...] = tuple(rule.evaluate(observation) for rule in self.rules)
        assessment = TimingAssessment(
            observation=observation,
            status=self.status_filter.classify(evaluations),
            evaluations=evaluations,
            assessed_at=self.clock(),
            confidence=self.status_filter.confidence(evaluations),
        )
        self.event_bus.publish(Event(TIMING_CREATED, {"assessment": assessment, "observation": observation}))
        return assessment

    # A readable synonym for integrations that use evaluation terminology.
    evaluate = assess
