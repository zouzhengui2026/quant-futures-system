"""Decision policy orchestration and event publication."""

from __future__ import annotations

from dataclasses import dataclass, field

from quant_futures.alpha.models import AlphaCandidate, AlphaDirection
from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError

from .models import DecisionAction, DecisionIntent
from .policies import ThresholdDecisionPolicy
from .protocols import DecisionPolicy

DECISION_CREATED = EventType.DECISION_CREATED


@dataclass(slots=True)
class DecisionEngine:
    """Generate one validated, non-executable decision proposal."""

    event_bus: EventBus
    policy: DecisionPolicy = field(default_factory=ThresholdDecisionPolicy)

    def decide(self, candidate: AlphaCandidate) -> DecisionIntent:
        if not isinstance(candidate, AlphaCandidate):
            raise TypeError("decide expects an AlphaCandidate")
        policy_name = getattr(self.policy, "name", None)
        if not isinstance(policy_name, str) or not policy_name.strip():
            raise DomainValidationError("DecisionPolicy.name must be a non-empty string")
        decision = self.policy.decide(candidate)
        if not isinstance(decision, DecisionIntent):
            raise TypeError("DecisionPolicy.decide must return a DecisionIntent")
        if decision.alpha_candidate != candidate:
            raise ValueError("DecisionPolicy output must reference the input alpha candidate")
        if decision.policy_name != policy_name:
            raise ValueError("DecisionPolicy output policy_name must match DecisionPolicy.name")
        if decision.symbol != candidate.symbol or decision.source != candidate.source:
            raise ValueError("DecisionPolicy output symbol and source must match alpha candidate")
        if candidate.direction is AlphaDirection.NEUTRAL and decision.action is not DecisionAction.ABSTAIN:
            raise ValueError("DecisionPolicy output must abstain for a NEUTRAL alpha candidate")
        if candidate.direction is AlphaDirection.LONG and decision.action is DecisionAction.PROPOSE_SHORT:
            raise ValueError("DecisionPolicy output action must match LONG alpha direction")
        if candidate.direction is AlphaDirection.SHORT and decision.action is DecisionAction.PROPOSE_LONG:
            raise ValueError("DecisionPolicy output action must match SHORT alpha direction")
        if decision.strength > candidate.strength or decision.confidence > candidate.confidence:
            raise ValueError("DecisionPolicy output must not amplify alpha strength or confidence")

        self.event_bus.publish(
            Event(
                DECISION_CREATED,
                {
                    "decision": decision,
                    "alpha_candidate": candidate,
                    "timing_assessment": candidate.timing_assessment,
                    "observation": candidate.observation,
                },
            )
        )
        return decision
