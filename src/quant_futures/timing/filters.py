"""Aggregation policy for timing-rule results."""

from __future__ import annotations

from collections.abc import Sequence

from .models import RuleEvaluation, RuleOutcome, TimingStatus


class TimingStatusFilter:
    """Map rule outcomes to a market-environment timing status.

    Any failed safety condition makes the environment unfavorable.  Otherwise,
    all passing conditions are favorable; partial confirmation is watching; and
    no confirmation is waiting.
    """

    def classify(self, evaluations: Sequence[RuleEvaluation]) -> TimingStatus:
        if not evaluations:
            raise ValueError("at least one rule evaluation is required")
        if not all(isinstance(evaluation, RuleEvaluation) for evaluation in evaluations):
            raise TypeError("evaluations must contain RuleEvaluation instances")
        if any(evaluation.outcome is RuleOutcome.FAIL for evaluation in evaluations):
            return TimingStatus.UNFAVORABLE
        if all(evaluation.outcome is RuleOutcome.PASS for evaluation in evaluations):
            return TimingStatus.FAVORABLE
        if any(evaluation.outcome is RuleOutcome.PASS for evaluation in evaluations):
            return TimingStatus.WATCHING
        return TimingStatus.WAITING
