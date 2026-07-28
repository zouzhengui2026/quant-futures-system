"""Defensive orchestration for creating and publishing execution intentions."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError
from quant_futures.decision.models import DecisionAction
from quant_futures.domain.order import OrderSide, OrderStatus
from quant_futures.risk.models import RiskAssessment, RiskOutcome

from .models import ExecutionIntent
from .policies import FixedQuantityExecutionPolicy
from .protocols import ExecutionPolicy

EXECUTION_UPDATED = EventType.EXECUTION_UPDATED


@dataclass(slots=True)
class ExecutionEngine:
    event_bus: EventBus
    policy: ExecutionPolicy = field(default_factory=FixedQuantityExecutionPolicy)

    def create_intent(self, risk_assessment: RiskAssessment) -> ExecutionIntent:
        if not isinstance(risk_assessment, RiskAssessment):
            raise TypeError("create_intent expects a RiskAssessment")
        risk_assessment.validate()
        if risk_assessment.outcome is not RiskOutcome.APPROVED:
            raise DomainValidationError("only an APPROVED RiskAssessment may create an intent")

        snapshot = deepcopy(risk_assessment)
        risk_before = risk_assessment
        decision_before = risk_before.decision_intent
        alpha_before = decision_before.alpha_candidate
        timing_before = alpha_before.timing_assessment
        observation_before = alpha_before.observation
        policy_before = self.policy
        name_before = self._validated_policy_name(policy_before)

        intent = policy_before.create_intent(risk_assessment)

        if self.policy is not policy_before:
            raise DomainValidationError("ExecutionEngine.policy must not change during create_intent")
        name_after = self._validated_policy_name(policy_before)
        if name_after != name_before:
            raise DomainValidationError("ExecutionPolicy.name must not change during create_intent")
        risk_assessment.validate()
        if risk_assessment != snapshot:
            raise DomainValidationError("ExecutionPolicy must not mutate RiskAssessment or its lineage")
        if risk_assessment.decision_intent is not decision_before:
            raise DomainValidationError("ExecutionPolicy must not replace DecisionIntent")
        if decision_before.alpha_candidate is not alpha_before:
            raise DomainValidationError("ExecutionPolicy must not replace AlphaCandidate")
        if alpha_before.timing_assessment is not timing_before:
            raise DomainValidationError("ExecutionPolicy must not replace TimingAssessment")
        if alpha_before.observation is not observation_before:
            raise DomainValidationError("ExecutionPolicy must not replace MarketObservation")
        if not isinstance(intent, ExecutionIntent):
            raise TypeError("ExecutionPolicy.create_intent must return an ExecutionIntent")
        intent.validate()
        if intent.risk_assessment is not risk_before:
            raise DomainValidationError("ExecutionIntent must reference the input RiskAssessment object")
        if intent.policy_name != name_after:
            raise DomainValidationError("ExecutionIntent.policy_name must match ExecutionPolicy.name")
        if intent.symbol != risk_before.symbol or intent.source != risk_before.source:
            raise DomainValidationError("ExecutionIntent symbol and source must match RiskAssessment")
        expected_side = {
            DecisionAction.PROPOSE_LONG: OrderSide.BUY,
            DecisionAction.PROPOSE_SHORT: OrderSide.SELL,
        }.get(decision_before.action)
        if expected_side is None or intent.order.side is not expected_side:
            raise DomainValidationError("Order side must match DecisionAction")
        if intent.order.status is not OrderStatus.CREATED:
            raise DomainValidationError("Order status must be CREATED")

        self.event_bus.publish(
            Event(
                EXECUTION_UPDATED,
                {
                    "execution_intent": intent,
                    "order": intent.order,
                    "risk_assessment": risk_before,
                    "decision_intent": decision_before,
                    "alpha_candidate": alpha_before,
                    "timing_assessment": timing_before,
                    "observation": observation_before,
                },
            )
        )
        return intent

    @staticmethod
    def _validated_policy_name(policy: object) -> str:
        name = getattr(policy, "name", None)
        if not isinstance(name, str) or not name.strip():
            raise DomainValidationError("ExecutionPolicy.name must be a non-empty string")
        return name
