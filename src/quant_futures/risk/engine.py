"""Risk-policy orchestration and publication of complete audit records."""

from __future__ import annotations

from dataclasses import dataclass, field

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError
from quant_futures.decision.models import DecisionAction, DecisionIntent
from quant_futures.timing.models import TimingStatus

from .models import RiskAssessment, RiskOutcome
from .policies import ThresholdRiskPolicy
from .protocols import RiskPolicy

RISK_UPDATED = EventType.RISK_UPDATED


@dataclass(slots=True)
class RiskEngine:
    event_bus: EventBus
    policy: RiskPolicy = field(default_factory=ThresholdRiskPolicy)

    def assess(self, decision: DecisionIntent) -> RiskAssessment:
        if not isinstance(decision, DecisionIntent):
            raise TypeError("assess expects a DecisionIntent")
        decision.validate()
        name_before = self._policy_name()
        assessment = self.policy.assess(decision)
        name_after = self._policy_name()
        if name_before != name_after:
            raise DomainValidationError("RiskPolicy.name must not change during assess")
        if not isinstance(assessment, RiskAssessment):
            raise TypeError("RiskPolicy.assess must return a RiskAssessment")
        assessment.validate()
        if assessment.decision_intent is not decision:
            raise DomainValidationError("RiskPolicy output must reference the input DecisionIntent object")
        if assessment.policy_name != name_after:
            raise DomainValidationError("RiskPolicy output policy_name must match RiskPolicy.name")
        if assessment.symbol != decision.symbol or assessment.source != decision.source:
            raise DomainValidationError("RiskPolicy output symbol and source must match decision")
        if decision.action is DecisionAction.ABSTAIN and assessment.outcome is RiskOutcome.APPROVED:
            raise DomainValidationError("ABSTAIN decisions may not be approved")
        timing = decision.alpha_candidate.timing_assessment
        if timing.status is TimingStatus.UNFAVORABLE and assessment.outcome is RiskOutcome.APPROVED:
            raise DomainValidationError("unfavorable timing may not be approved")

        self.event_bus.publish(
            Event(
                RISK_UPDATED,
                {
                    "risk_assessment": assessment,
                    "decision_intent": decision,
                    "alpha_candidate": decision.alpha_candidate,
                    "timing_assessment": timing,
                    "observation": decision.alpha_candidate.observation,
                },
            )
        )
        return assessment

    def _policy_name(self) -> str:
        name = getattr(self.policy, "name", None)
        if not isinstance(name, str) or not name.strip():
            raise DomainValidationError("RiskPolicy.name must be a non-empty string")
        return name
