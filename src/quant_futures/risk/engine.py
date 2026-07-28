"""Risk-policy orchestration and publication of complete audit records."""

from __future__ import annotations

from copy import deepcopy
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

        decision_snapshot = deepcopy(decision)
        candidate_before = decision.alpha_candidate
        timing_before = candidate_before.timing_assessment
        observation_before = candidate_before.observation
        policy_before = self.policy
        name_before = self._validated_policy_name(policy_before)

        assessment = policy_before.assess(decision)

        if self.policy is not policy_before:
            raise DomainValidationError("RiskEngine.policy must not change during assess")
        name_after = self._validated_policy_name(policy_before)
        if name_before != name_after:
            raise DomainValidationError("RiskPolicy.name must not change during assess")
        decision.validate()
        if decision != decision_snapshot:
            raise DomainValidationError("RiskPolicy must not mutate the input DecisionIntent or its lineage")
        if decision.alpha_candidate is not candidate_before:
            raise DomainValidationError("RiskPolicy must not replace the input DecisionIntent alpha_candidate")
        if decision.alpha_candidate.timing_assessment is not timing_before:
            raise DomainValidationError("RiskPolicy must not replace the input DecisionIntent timing_assessment")
        if decision.alpha_candidate.observation is not observation_before:
            raise DomainValidationError("RiskPolicy must not replace the input DecisionIntent observation")
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

    @staticmethod
    def _validated_policy_name(policy: object) -> str:
        name = getattr(policy, "name", None)
        if not isinstance(name, str) or not name.strip():
            raise DomainValidationError("RiskPolicy.name must be a non-empty string")
        return name
