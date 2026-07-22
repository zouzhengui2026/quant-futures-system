from dataclasses import fields
from datetime import datetime, timedelta, timezone

import pytest

from quant_futures.alpha import AlphaCandidate, AlphaDirection
from quant_futures.core.events import EventBus, EventType
from quant_futures.decision import DecisionAction, DecisionEngine, DecisionIntent, ThresholdDecisionPolicy
from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.models import LiquidityRegime, MarketObservation, TrendRegime, VolatilityRegime
from quant_futures.timing import RuleEvaluation, RuleOutcome, TimingAssessment, TimingStatus


GENERATED_AT = datetime(2026, 1, 1, 2, tzinfo=timezone.utc)
CREATED_AT = GENERATED_AT + timedelta(minutes=1)


def candidate_for(
    direction: AlphaDirection = AlphaDirection.LONG,
    status: TimingStatus = TimingStatus.FAVORABLE,
    **overrides: object,
) -> AlphaCandidate:
    observation = MarketObservation(
        symbol="BTCUSDT", source="replay", timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc), price=60_000.0,
        trend_regime=TrendRegime.UPTREND, volatility_regime=VolatilityRegime.NORMAL,
        liquidity_regime=LiquidityRegime.LIQUID, features=MarketFeatures(trend_strength=0.8),
    )
    assessment = TimingAssessment(
        observation=observation, status=status, evaluations=(RuleEvaluation("test", RuleOutcome.PASS, "test"),),
        assessed_at=datetime(2026, 1, 1, 1, tzinfo=timezone.utc), confidence=0.8,
    )
    values: dict[str, object] = dict(
        symbol="BTCUSDT", source="replay", observation_timestamp=observation.timestamp, generated_at=GENERATED_AT,
        model_name="test", direction=direction, strength=0.8 if direction is not AlphaDirection.NEUTRAL else 0.0,
        confidence=0.8, reasons=("test alpha",), observation=observation, timing_assessment=assessment,
    )
    values.update(overrides)
    return AlphaCandidate(**values)  # type: ignore[arg-type]


def intent_for(candidate: AlphaCandidate, **overrides: object) -> DecisionIntent:
    action = {AlphaDirection.LONG: DecisionAction.PROPOSE_LONG, AlphaDirection.SHORT: DecisionAction.PROPOSE_SHORT,
              AlphaDirection.NEUTRAL: DecisionAction.ABSTAIN}[candidate.direction]
    values: dict[str, object] = dict(symbol=candidate.symbol, source=candidate.source, created_at=CREATED_AT,
        policy_name="test", action=action, strength=candidate.strength if action is not DecisionAction.ABSTAIN else 0.0,
        confidence=candidate.confidence if action is not DecisionAction.ABSTAIN else 0.0,
        reasons=("test decision",), alpha_candidate=candidate)
    values.update(overrides)
    return DecisionIntent(**values)  # type: ignore[arg-type]


def test_intent_is_immutable_and_has_no_trading_or_risk_fields() -> None:
    intent = intent_for(candidate_for())
    assert intent.action is DecisionAction.PROPOSE_LONG
    with pytest.raises(AttributeError):
        intent.strength = 0.1  # type: ignore[misc]
    assert not {field.name for field in fields(DecisionIntent)}.intersection(
        {"quantity", "notional", "position_size", "leverage", "entry_price", "limit_price", "stop_loss", "take_profit", "order_type", "time_in_force", "risk_approved", "account_balance", "exchange"}
    )


@pytest.mark.parametrize("field,value", [
    ("strength", -0.1), ("strength", 1.1), ("strength", float("nan")), ("strength", float("inf")),
    ("confidence", float("-inf")), ("confidence", True),
])
def test_intent_rejects_invalid_scores(field: str, value: object) -> None:
    with pytest.raises(ValueError, match="finite number"):
        intent_for(candidate_for(), **{field: value})


def test_intent_validates_time_reasons_and_candidate_identity() -> None:
    candidate = candidate_for()
    with pytest.raises(ValueError, match="timezone-aware"):
        intent_for(candidate, created_at=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="not be earlier"):
        intent_for(candidate, created_at=GENERATED_AT - timedelta(seconds=1))
    with pytest.raises(ValueError, match="non-empty tuple"):
        intent_for(candidate, reasons=())
    with pytest.raises(ValueError, match="symbol and source"):
        intent_for(candidate, symbol="ETHUSDT")


@pytest.mark.parametrize("field", ["strength", "confidence"])
def test_abstain_rejects_nonzero_scores(field: str) -> None:
    candidate = candidate_for()
    with pytest.raises(ValueError, match="ABSTAIN"):
        intent_for(candidate, action=DecisionAction.ABSTAIN, **{field: 0.1})


def test_directional_and_neutral_action_rules() -> None:
    assert intent_for(candidate_for(AlphaDirection.LONG)).action is DecisionAction.PROPOSE_LONG
    assert intent_for(candidate_for(AlphaDirection.SHORT)).action is DecisionAction.PROPOSE_SHORT
    neutral = candidate_for(AlphaDirection.NEUTRAL)
    assert intent_for(neutral).action is DecisionAction.ABSTAIN
    with pytest.raises(ValueError):
        intent_for(neutral, action=DecisionAction.PROPOSE_LONG, strength=0.1)


def test_intent_cannot_amplify_alpha_scores() -> None:
    candidate = candidate_for(strength=0.5, confidence=0.6)
    with pytest.raises(ValueError, match="must not exceed"):
        intent_for(candidate, strength=0.6)
    with pytest.raises(ValueError, match="must not exceed"):
        intent_for(candidate, confidence=0.7)


def test_baseline_abstains_for_unfavorable_neutral_and_threshold_failures() -> None:
    policy = ThresholdDecisionPolicy(clock=lambda: CREATED_AT)
    unfavorable = policy.decide(candidate_for(status=TimingStatus.UNFAVORABLE))
    neutral = policy.decide(candidate_for(AlphaDirection.NEUTRAL))
    weak = policy.decide(candidate_for(strength=0.4, confidence=0.4))
    assert all(item.action is DecisionAction.ABSTAIN and item.strength == item.confidence == 0.0 for item in (unfavorable, neutral, weak))
    assert "strength, confidence" in weak.reasons[0]
    assert "actual strength=0.4" in weak.reasons[0]
    assert "required confidence=0.5" in weak.reasons[0]


def test_baseline_passes_exact_threshold_without_amplifying_and_is_deterministic() -> None:
    policy = ThresholdDecisionPolicy(clock=lambda: CREATED_AT)
    candidate = candidate_for(strength=0.5, confidence=0.5)
    first, second = policy.decide(candidate), policy.decide(candidate)
    assert first == second
    assert first.action is DecisionAction.PROPOSE_LONG
    assert (first.strength, first.confidence) == (candidate.strength, candidate.confidence)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True, "invalid", -0.1, 1.1])
def test_threshold_configuration_fails_closed(value: object) -> None:
    with pytest.raises(ValueError, match="finite number"):
        ThresholdDecisionPolicy(minimum_strength=value)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="finite number"):
        ThresholdDecisionPolicy(minimum_confidence=value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("minimum_strength", float("nan"), "minimum_strength"),
        ("minimum_confidence", float("inf"), "minimum_confidence"),
        ("minimum_strength", True, "minimum_strength"),
        ("minimum_confidence", "invalid", "minimum_confidence"),
        ("minimum_strength", -0.1, "minimum_strength"),
        ("minimum_confidence", 1.1, "minimum_confidence"),
        ("name", "", "name must be a non-empty string"),
        ("clock", None, "clock must be callable"),
    ],
)
def test_threshold_policy_revalidates_tampered_runtime_configuration(field: str, value: object, message: str) -> None:
    policy = ThresholdDecisionPolicy(clock=lambda: CREATED_AT)
    with pytest.raises(AttributeError):
        setattr(policy, field, value)
    object.__setattr__(policy, field, value)
    with pytest.raises(ValueError, match=message):
        policy.decide(candidate_for())


@pytest.mark.parametrize("field", ["minimum_strength", "minimum_confidence"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf"), True, "invalid", -0.1, 1.1])
def test_threshold_policy_rejects_every_invalid_runtime_threshold(field: str, value: object) -> None:
    policy = ThresholdDecisionPolicy(clock=lambda: CREATED_AT)
    object.__setattr__(policy, field, value)
    with pytest.raises(ValueError, match=field):
        policy.decide(candidate_for())


def test_engine_publishes_complete_canonical_event_and_accepts_injected_policy() -> None:
    class CustomPolicy:
        name = "custom"
        def decide(self, candidate: AlphaCandidate) -> DecisionIntent:
            return intent_for(candidate, policy_name=self.name)

    bus, received, candidate = EventBus(), [], candidate_for()
    bus.subscribe(EventType.DECISION_CREATED, received.append)
    decision = DecisionEngine(bus, policy=CustomPolicy()).decide(candidate)
    assert received[0].event_type is EventType.DECISION_CREATED
    assert received[0].payload == {"decision": decision, "alpha_candidate": candidate,
                                   "timing_assessment": candidate.timing_assessment, "observation": candidate.observation}


def test_engine_rejects_invalid_input_or_policy_output_without_publishing() -> None:
    bus, received = EventBus(), []
    bus.subscribe(EventType.DECISION_CREATED, received.append)
    with pytest.raises(TypeError, match="AlphaCandidate"):
        DecisionEngine(bus).decide(None)  # type: ignore[arg-type]

    class BadName:
        name = ""
        def decide(self, candidate: AlphaCandidate) -> DecisionIntent: return intent_for(candidate)
    with pytest.raises(ValueError, match="DecisionPolicy.name"):
        DecisionEngine(bus, policy=BadName()).decide(candidate_for())

    class ForgedName:
        name = "configured"
        def decide(self, candidate: AlphaCandidate) -> DecisionIntent: return intent_for(candidate, policy_name="forged")
    with pytest.raises(ValueError, match="policy_name must match"):
        DecisionEngine(bus, policy=ForgedName()).decide(candidate_for())

    class OtherCandidate:
        name = "other"
        def decide(self, candidate: AlphaCandidate) -> DecisionIntent:
            other = candidate_for(generated_at=GENERATED_AT - timedelta(hours=1))
            return intent_for(other, policy_name=self.name)
    with pytest.raises(ValueError, match="input alpha candidate"):
        DecisionEngine(bus, policy=OtherCandidate()).decide(candidate_for())
    assert received == []


def test_engine_rejects_forged_direction_or_amplified_output_without_publishing() -> None:
    bus, received, candidate = EventBus(), [], candidate_for()
    bus.subscribe(EventType.DECISION_CREATED, received.append)

    class ForgedAction:
        name = "forged-action"
        def decide(self, value: AlphaCandidate) -> DecisionIntent:
            decision = intent_for(value, policy_name=self.name)
            object.__setattr__(decision, "action", DecisionAction.PROPOSE_SHORT)
            return decision
    with pytest.raises(ValueError, match="PROPOSE_SHORT requires a SHORT"):
        DecisionEngine(bus, policy=ForgedAction()).decide(candidate)

    class Amplified:
        name = "amplified"
        def decide(self, value: AlphaCandidate) -> DecisionIntent:
            decision = intent_for(value, policy_name=self.name)
            object.__setattr__(decision, "strength", 0.9)
            return decision
    with pytest.raises(ValueError, match="must not exceed"):
        DecisionEngine(bus, policy=Amplified()).decide(candidate)
    assert received == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("strength", 0.1, "ABSTAIN"),
        ("confidence", 0.1, "ABSTAIN"),
        ("reasons", (), "non-empty tuple"),
        ("reasons", ("",), "non-empty strings"),
        ("created_at", datetime(2026, 1, 1), "timezone-aware"),
        ("created_at", GENERATED_AT - timedelta(seconds=1), "not be earlier"),
        ("strength", float("nan"), "finite number"),
        ("confidence", float("inf"), "finite number"),
        ("symbol", "ETHUSDT", "symbol and source"),
        ("source", "other", "symbol and source"),
    ],
)
def test_engine_revalidates_tampered_intent_without_publishing(field: str, value: object, message: str) -> None:
    candidate = candidate_for()
    bus, received = EventBus(), []
    bus.subscribe(EventType.DECISION_CREATED, received.append)

    class TamperingPolicy:
        name = "tampering"

        def decide(self, supplied: AlphaCandidate) -> DecisionIntent:
            decision = intent_for(supplied, policy_name=self.name, action=DecisionAction.ABSTAIN, strength=0.0, confidence=0.0)
            object.__setattr__(decision, field, value)
            return decision

    with pytest.raises(ValueError, match=message):
        DecisionEngine(bus, policy=TamperingPolicy()).decide(candidate)
    assert received == []


def test_engine_rejects_unfavorable_directional_policy_output_without_publishing() -> None:
    candidate = candidate_for(status=TimingStatus.UNFAVORABLE)
    bus, received = EventBus(), []
    bus.subscribe(EventType.DECISION_CREATED, received.append)

    class UnsafePolicy:
        name = "unsafe"

        def decide(self, supplied: AlphaCandidate) -> DecisionIntent:
            decision = intent_for(supplied, policy_name=self.name, action=DecisionAction.ABSTAIN, strength=0.0, confidence=0.0)
            object.__setattr__(decision, "action", DecisionAction.PROPOSE_LONG)
            object.__setattr__(decision, "strength", supplied.strength)
            object.__setattr__(decision, "confidence", supplied.confidence)
            return decision

    with pytest.raises(ValueError, match="unfavorable timing"):
        DecisionEngine(bus, policy=UnsafePolicy()).decide(candidate)
    assert received == []
