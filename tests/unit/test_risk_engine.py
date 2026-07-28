from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone, tzinfo

import pytest

from quant_futures.alpha import AlphaCandidate, AlphaDirection
from quant_futures.core.events import EventBus, EventType
from quant_futures.decision import DecisionAction, DecisionIntent
from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.models import (
    LiquidityRegime,
    MarketObservation,
    TrendRegime,
    VolatilityRegime,
)
from quant_futures.risk import RiskAssessment, RiskEngine, RiskOutcome, ThresholdRiskPolicy
from quant_futures.timing import RuleEvaluation, RuleOutcome, TimingAssessment, TimingStatus

GENERATED = datetime(2026, 1, 1, 2, tzinfo=timezone.utc)
CREATED = GENERATED + timedelta(minutes=1)
ASSESSED = CREATED + timedelta(minutes=1)


def decision_for(
    direction: AlphaDirection = AlphaDirection.LONG,
    status: TimingStatus = TimingStatus.FAVORABLE,
    strength: float = 0.8,
    confidence: float = 0.8,
) -> DecisionIntent:
    observation = MarketObservation(
        symbol="BTCUSDT", source="replay", timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        price=60_000.0, trend_regime=TrendRegime.UPTREND, volatility_regime=VolatilityRegime.NORMAL,
        liquidity_regime=LiquidityRegime.LIQUID, features=MarketFeatures(trend_strength=0.8),
    )
    timing = TimingAssessment(
        observation, status, (RuleEvaluation("test", RuleOutcome.PASS, "test"),),
        datetime(2026, 1, 1, 1, tzinfo=timezone.utc), 0.8,
    )
    candidate = AlphaCandidate(
        "BTCUSDT", "replay", observation.timestamp, GENERATED, "test", direction,
        strength if direction is not AlphaDirection.NEUTRAL else 0.0, confidence,
        ("alpha",), observation, timing,
    )
    action = {
        AlphaDirection.LONG: DecisionAction.PROPOSE_LONG,
        AlphaDirection.SHORT: DecisionAction.PROPOSE_SHORT,
        AlphaDirection.NEUTRAL: DecisionAction.ABSTAIN,
    }[direction]
    if status is TimingStatus.UNFAVORABLE:
        action = DecisionAction.ABSTAIN
    return DecisionIntent(
        candidate.symbol, candidate.source, CREATED, "decision", action,
        0.0 if action is DecisionAction.ABSTAIN else strength,
        0.0 if action is DecisionAction.ABSTAIN else confidence,
        ("decision",), candidate,
    )


def assessment_for(decision: DecisionIntent, **changes: object) -> RiskAssessment:
    values = dict(
        symbol=decision.symbol, source=decision.source, assessed_at=ASSESSED,
        policy_name="risk", outcome=RiskOutcome.APPROVED,
        reasons=("reviewed",), decision_intent=decision,
    )
    values.update(changes)
    return RiskAssessment(**values)  # type: ignore[arg-type]


def test_assessment_is_frozen_slotted_and_contains_no_execution_fields() -> None:
    assessment = assessment_for(decision_for())
    with pytest.raises((FrozenInstanceError, AttributeError)):
        assessment.source = "changed"  # type: ignore[misc]
    prohibited = {
        "quantity", "size", "position_size", "notional", "leverage", "price", "entry_price",
        "limit_price", "stop_loss", "take_profit", "order", "order_type", "time_in_force",
        "account_balance", "exchange", "execution",
    }
    assert not prohibited.intersection(field.name for field in fields(RiskAssessment))
    assert not hasattr(assessment, "__dict__")
    assessment.validate()


@pytest.mark.parametrize("field,value", [("symbol", ""), ("source", " "), ("policy_name", 1)])
def test_assessment_rejects_invalid_strings(field: str, value: object) -> None:
    with pytest.raises(ValueError, match="non-empty string"):
        assessment_for(decision_for(), **{field: value})


class NoneOffset(tzinfo):
    def utcoffset(self, dt: datetime | None) -> None:
        return None


@pytest.mark.parametrize("when", [datetime(2026, 1, 1), datetime(2026, 1, 1, tzinfo=NoneOffset())])
def test_assessment_requires_effectively_aware_time(when: datetime) -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        assessment_for(decision_for(), assessed_at=when)


def test_assessment_rejects_invalid_lineage_reasons_outcome_and_time() -> None:
    decision = decision_for()
    with pytest.raises(ValueError, match="earlier"):
        assessment_for(decision, assessed_at=CREATED - timedelta(seconds=1))
    for reasons in ((), ("",)):
        with pytest.raises(ValueError, match="reasons"):
            assessment_for(decision, reasons=reasons)
    with pytest.raises(ValueError, match="RiskOutcome"):
        assessment_for(decision, outcome="approved")
    with pytest.raises(ValueError, match="DecisionIntent"):
        assessment_for(decision, decision_intent=None)
    with pytest.raises(ValueError, match="symbol and source"):
        assessment_for(decision, symbol="ETHUSDT")


def test_assessment_rejects_approval_for_abstain_and_unfavorable() -> None:
    for decision in (decision_for(AlphaDirection.NEUTRAL), decision_for(status=TimingStatus.UNFAVORABLE)):
        with pytest.raises(ValueError, match="directional proposal"):
            assessment_for(decision)
        rejected = assessment_for(decision, outcome=RiskOutcome.REJECTED)
        assert rejected.outcome is RiskOutcome.REJECTED


def test_assessment_revalidation_fails_closed_after_tampering() -> None:
    assessment = assessment_for(decision_for())
    object.__setattr__(assessment, "outcome", "approved")
    with pytest.raises(ValueError, match="RiskOutcome"):
        assessment.validate()


def test_threshold_policy_approves_directions_at_threshold_deterministically() -> None:
    policy = ThresholdRiskPolicy(clock=lambda: ASSESSED)
    for direction in (AlphaDirection.LONG, AlphaDirection.SHORT):
        decision = decision_for(direction, strength=0.5, confidence=0.5)
        assert policy.assess(decision) == policy.assess(decision)
        assert policy.assess(decision).outcome is RiskOutcome.APPROVED


def test_threshold_policy_rejects_abstain_and_threshold_failures_with_audit_values() -> None:
    policy = ThresholdRiskPolicy(clock=lambda: ASSESSED)
    assert policy.assess(decision_for(AlphaDirection.NEUTRAL)).outcome is RiskOutcome.REJECTED
    result = policy.assess(decision_for(strength=0.4, confidence=0.3))
    assert result.outcome is RiskOutcome.REJECTED
    assert "actual=0.4, required=0.5" in result.reasons[0]
    assert "actual=0.3, required=0.5" in result.reasons[1]


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True, "0.5", -0.1, 1.1])
def test_threshold_policy_rejects_invalid_configuration_and_tampering(value: object) -> None:
    with pytest.raises(ValueError, match="finite number"):
        ThresholdRiskPolicy(minimum_strength=value)  # type: ignore[arg-type]
    policy = ThresholdRiskPolicy(clock=lambda: ASSESSED)
    object.__setattr__(policy, "minimum_confidence", value)
    with pytest.raises(ValueError, match="minimum_confidence"):
        policy.assess(decision_for())


@pytest.mark.parametrize("field,value", [("name", ""), ("clock", None)])
def test_threshold_policy_revalidates_name_and_clock(field: str, value: object) -> None:
    policy = ThresholdRiskPolicy(clock=lambda: ASSESSED)
    object.__setattr__(policy, field, value)
    with pytest.raises(ValueError):
        policy.assess(decision_for())


def test_policy_rejects_non_decision() -> None:
    with pytest.raises(TypeError, match="DecisionIntent"):
        ThresholdRiskPolicy().assess(None)  # type: ignore[arg-type]


def test_engine_default_policy_publishes_exact_canonical_lineage() -> None:
    bus, events, decision = EventBus(), [], decision_for()
    bus.subscribe(EventType.RISK_UPDATED, events.append)
    result = RiskEngine(bus, ThresholdRiskPolicy(clock=lambda: ASSESSED)).assess(decision)
    assert events[0].event_type is EventType.RISK_UPDATED
    assert events[0].payload == {
        "risk_assessment": result, "decision_intent": decision,
        "alpha_candidate": decision.alpha_candidate,
        "timing_assessment": decision.alpha_candidate.timing_assessment,
        "observation": decision.alpha_candidate.observation,
    }
    assert events[0].payload["risk_assessment"] is result


def test_engine_rejects_malicious_policy_outputs_without_event() -> None:
    decision, bus, events = decision_for(), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)

    class Policy:
        name = "risk"
        def __init__(self, result: object) -> None: self.result = result
        def assess(self, supplied: DecisionIntent) -> object: return self.result

    invalid: list[object] = [None]
    clone = replace(decision)
    invalid.extend([
        assessment_for(clone),
        assessment_for(decision, policy_name="forged"),
    ])
    tampered = assessment_for(decision)
    object.__setattr__(tampered, "reasons", ())
    invalid.append(tampered)
    for output in invalid:
        with pytest.raises((TypeError, ValueError)):
            RiskEngine(bus, Policy(output)).assess(decision)  # type: ignore[arg-type]
    assert events == []


def test_engine_rejects_renaming_policy_and_tampered_decision_without_event() -> None:
    bus, events, decision = EventBus(), [], decision_for()
    bus.subscribe(EventType.RISK_UPDATED, events.append)

    class Renaming:
        name = "risk"
        def assess(self, supplied: DecisionIntent) -> RiskAssessment:
            result = assessment_for(supplied)
            self.name = "changed"
            return result

    with pytest.raises(ValueError, match="must not change"):
        RiskEngine(bus, Renaming()).assess(decision)
    object.__setattr__(decision, "symbol", "ETHUSDT")
    with pytest.raises(ValueError):
        RiskEngine(bus).assess(decision)
    assert events == []


@pytest.mark.parametrize(
    "mutation",
    [
        lambda decision: object.__setattr__(decision, "reasons", ("rewritten",)),
        lambda decision: object.__setattr__(decision, "policy_name", "rewritten_policy"),
        lambda decision: object.__setattr__(decision.alpha_candidate, "reasons", ("rewritten alpha",)),
        lambda decision: object.__setattr__(
            decision, "alpha_candidate", replace(decision.alpha_candidate)
        ),
        lambda decision: object.__setattr__(
            decision.alpha_candidate,
            "timing_assessment",
            replace(decision.alpha_candidate.timing_assessment),
        ),
    ],
    ids=["decision-reasons", "decision-policy-name", "alpha-reasons", "alpha-clone", "timing-clone"],
)
def test_engine_rejects_policy_mutation_of_decision_lineage_without_event(mutation: object) -> None:
    decision, bus, events = decision_for(), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)

    class MutatingPolicy:
        name = "risk"

        def assess(self, supplied: DecisionIntent) -> RiskAssessment:
            mutation(supplied)  # type: ignore[operator]
            return assessment_for(supplied)

    with pytest.raises(ValueError, match="must not mutate|must not replace"):
        RiskEngine(bus, MutatingPolicy()).assess(decision)
    assert events == []


def test_engine_rejects_equal_observation_replacement_without_event() -> None:
    decision, bus, events = decision_for(), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)

    class ObservationReplacingPolicy:
        name = "risk"

        def assess(self, supplied: DecisionIntent) -> RiskAssessment:
            candidate = supplied.alpha_candidate
            observation_clone = replace(candidate.observation)
            timing_clone = replace(candidate.timing_assessment, observation=observation_clone)
            object.__setattr__(candidate, "observation", observation_clone)
            object.__setattr__(candidate, "timing_assessment", timing_clone)
            return assessment_for(supplied)

    with pytest.raises(ValueError, match="must not replace|must not mutate"):
        RiskEngine(bus, ObservationReplacingPolicy()).assess(decision)
    assert events == []


def test_engine_rejects_same_named_policy_object_replacement_without_event() -> None:
    decision, bus, events = decision_for(), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)

    class OtherPolicy:
        name = "risk"

    class ReplacementPolicy:
        name = "risk"

        def assess(self, supplied: DecisionIntent) -> RiskAssessment:
            result = assessment_for(supplied)
            engine.policy = OtherPolicy()  # type: ignore[assignment]
            return result

    engine = RiskEngine(bus, ReplacementPolicy())
    with pytest.raises(ValueError, match="RiskEngine.policy must not change"):
        engine.assess(decision)
    assert events == []


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("symbol", "ETHUSDT"),
        ("source", "forged"),
        ("assessed_at", datetime(2026, 1, 1)),
        ("assessed_at", CREATED - timedelta(seconds=1)),
        ("outcome", "approved"),
        ("reasons", ()),
        ("reasons", ("",)),
    ],
)
def test_engine_revalidates_tampered_assessment_fields_without_event(field: str, value: object) -> None:
    decision, bus, events = decision_for(), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)
    output = assessment_for(decision)
    object.__setattr__(output, field, value)

    class Policy:
        name = "risk"
        def assess(self, supplied: DecisionIntent) -> RiskAssessment: return output

    with pytest.raises(ValueError):
        RiskEngine(bus, Policy()).assess(decision)
    assert events == []


@pytest.mark.parametrize("status", [TimingStatus.FAVORABLE, TimingStatus.UNFAVORABLE])
def test_engine_rejects_forged_approval_and_replaced_decision_without_event(status: TimingStatus) -> None:
    direction = AlphaDirection.NEUTRAL if status is TimingStatus.FAVORABLE else AlphaDirection.LONG
    decision, bus, events = decision_for(direction, status=status), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)
    output = assessment_for(decision, outcome=RiskOutcome.REJECTED)
    object.__setattr__(output, "outcome", RiskOutcome.APPROVED)

    class Policy:
        name = "risk"
        def assess(self, supplied: DecisionIntent) -> RiskAssessment: return output

    with pytest.raises(ValueError):
        RiskEngine(bus, Policy()).assess(decision)
    assert events == []


def test_engine_rejects_tampered_assessment_decision_reference_without_event() -> None:
    decision, bus, events = decision_for(), EventBus(), []
    bus.subscribe(EventType.RISK_UPDATED, events.append)
    output = assessment_for(decision)
    object.__setattr__(output, "decision_intent", replace(decision))

    class Policy:
        name = "risk"
        def assess(self, supplied: DecisionIntent) -> RiskAssessment: return output

    with pytest.raises(ValueError, match="input DecisionIntent object"):
        RiskEngine(bus, Policy()).assess(decision)
    assert events == []
