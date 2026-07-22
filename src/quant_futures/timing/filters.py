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
        self._validate_evaluations(evaluations)
        if any(evaluation.outcome is RuleOutcome.FAIL for evaluation in evaluations):
            return TimingStatus.UNFAVORABLE
        if all(evaluation.outcome is RuleOutcome.PASS for evaluation in evaluations):
            return TimingStatus.FAVORABLE
        if any(evaluation.outcome is RuleOutcome.PASS for evaluation in evaluations):
            return TimingStatus.WATCHING
        return TimingStatus.WAITING

    def confidence(self, evaluations: Sequence[RuleEvaluation]) -> float:
        """Return the deterministic mean readiness score of rule outcomes.

        Passing conditions contribute ``1.0``, neutral descriptions contribute
        ``0.5``, and blocking safety failures contribute ``0.0``.
        """
        self._validate_evaluations(evaluations)
        scores = {RuleOutcome.PASS: 1.0, RuleOutcome.NEUTRAL: 0.5, RuleOutcome.FAIL: 0.0}
        return sum(scores[evaluation.outcome] for evaluation in evaluations) / len(evaluations)

    @staticmethod
    def _validate_evaluations(evaluations: Sequence[RuleEvaluation]) -> None:
        if not evaluations:
            raise ValueError("at least one rule evaluation is required")
        if not all(isinstance(evaluation, RuleEvaluation) for evaluation in evaluations):
            raise TypeError("evaluations must contain RuleEvaluation instances")
