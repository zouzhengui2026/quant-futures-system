from dataclasses import FrozenInstanceError, fields, replace
from datetime import datetime, timedelta, timezone

import pytest

from quant_futures.alpha import AlphaCandidate, AlphaDirection
from quant_futures.core.events import EventBus, EventType
from quant_futures.decision import DecisionAction, DecisionIntent
from quant_futures.domain.order import OrderSide, OrderStatus
from quant_futures.execution import (
    EXECUTION_UPDATED,
    ExecutionEngine,
    ExecutionIntent,
    FixedQuantityExecutionPolicy,
)
from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.models import (
    LiquidityRegime,
    MarketObservation,
    TrendRegime,
    VolatilityRegime,
)
from quant_futures.risk import RiskAssessment, RiskOutcome
from quant_futures.timing import RuleEvaluation, RuleOutcome, TimingAssessment, TimingStatus

OBSERVED = datetime(2026, 1, 1, tzinfo=timezone.utc)
GENERATED = OBSERVED + timedelta(hours=2)
DECIDED = GENERATED + timedelta(minutes=1)
ASSESSED = DECIDED + timedelta(minutes=1)
CREATED = ASSESSED + timedelta(minutes=1)


def risk_for(direction: AlphaDirection = AlphaDirection.LONG, outcome: RiskOutcome = RiskOutcome.APPROVED) -> RiskAssessment:
    observation = MarketObservation(
        symbol="BTCUSDT", source="replay", timestamp=OBSERVED, price=60_000.0,
        trend_regime=TrendRegime.UPTREND, volatility_regime=VolatilityRegime.NORMAL,
        liquidity_regime=LiquidityRegime.LIQUID, features=MarketFeatures(trend_strength=0.8),
    )
    timing = TimingAssessment(
        observation, TimingStatus.FAVORABLE,
        (RuleEvaluation("test", RuleOutcome.PASS, "test"),),
        OBSERVED + timedelta(hours=1), 0.8,
    )
    candidate = AlphaCandidate(
        "BTCUSDT", "replay", OBSERVED, GENERATED, "alpha", direction,
        0.8 if direction is not AlphaDirection.NEUTRAL else 0.0, 0.8,
        ("alpha",), observation, timing,
    )
    action = {
        AlphaDirection.LONG: DecisionAction.PROPOSE_LONG,
        AlphaDirection.SHORT: DecisionAction.PROPOSE_SHORT,
        AlphaDirection.NEUTRAL: DecisionAction.ABSTAIN,
    }[direction]
    decision = DecisionIntent(
        "BTCUSDT", "replay", DECIDED, "decision", action,
        0.0 if action is DecisionAction.ABSTAIN else 0.8,
        0.0 if action is DecisionAction.ABSTAIN else 0.8,
        ("decision",), candidate,
    )
    return RiskAssessment(
        "BTCUSDT", "replay", ASSESSED, "risk", outcome, ("reviewed",), decision
    )


def policy(**changes: object) -> FixedQuantityExecutionPolicy:
    values = {"clock": lambda: CREATED, "order_id_factory": lambda: "order-1"}
    values.update(changes)
    return FixedQuantityExecutionPolicy(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "direction,side", [(AlphaDirection.LONG, OrderSide.BUY), (AlphaDirection.SHORT, OrderSide.SELL)]
)
def test_policy_maps_direction_and_creates_only_created_order(direction: AlphaDirection, side: OrderSide) -> None:
    risk = risk_for(direction)
    intent = policy(quantity=2.5).create_intent(risk)
    assert intent.risk_assessment is risk
    assert intent.order.side is side
    assert intent.order.quantity == 2.5
    assert intent.order.status is OrderStatus.CREATED
    assert intent.order.created_at is intent.created_at
    assert intent.order.price is None


def test_intent_is_frozen_slotted_revalidatable_and_has_nothing_executable() -> None:
    intent = policy(price=60_000.0).create_intent(risk_for())
    assert not hasattr(intent, "__dict__")
    with pytest.raises((FrozenInstanceError, AttributeError)):
        intent.source = "changed"  # type: ignore[misc]
    intent.validate()
    prohibited = {"exchange_client", "external_order_id", "fill_price", "filled_quantity", "fees", "position", "account_balance", "margin", "leverage", "submission_status", "retry_metadata"}
    assert not prohibited.intersection(field.name for field in fields(ExecutionIntent))


def test_policy_is_deterministic_and_calls_factories_once() -> None:
    calls = {"clock": 0, "id": 0}
    def clock() -> datetime:
        calls["clock"] += 1
        return CREATED
    def identifier() -> str:
        calls["id"] += 1
        return "fixed"
    risk = risk_for()
    first = FixedQuantityExecutionPolicy(clock=clock, order_id_factory=identifier).create_intent(risk)
    assert calls == {"clock": 1, "id": 1}
    calls = {"clock": 0, "id": 0}
    second = FixedQuantityExecutionPolicy(clock=clock, order_id_factory=identifier).create_intent(risk)
    assert first == second


CALLBACK_MUTATIONS = [
    ("clock", "quantity"),
    ("clock", "price"),
    ("clock", "name"),
    ("clock", "order_id_factory"),
    ("order_id_factory", "quantity"),
    ("order_id_factory", "price"),
    ("order_id_factory", "clock"),
]


def mutating_policy(phase: str, field: str) -> tuple[FixedQuantityExecutionPolicy, dict[str, int]]:
    holder: dict[str, FixedQuantityExecutionPolicy] = {}
    calls = {"clock": 0, "order_id_factory": 0}

    def replacement_clock() -> datetime:
        return CREATED

    def replacement_identifier() -> str:
        return "order-1"

    replacements: dict[str, object] = {
        "quantity": 2.0,
        "price": 60_000.0,
        "name": "changed_but_valid",
        "clock": replacement_clock,
        "order_id_factory": replacement_identifier,
    }

    def clock() -> datetime:
        calls["clock"] += 1
        if phase == "clock":
            object.__setattr__(holder["policy"], field, replacements[field])
        return CREATED

    def identifier() -> str:
        calls["order_id_factory"] += 1
        if phase == "order_id_factory":
            object.__setattr__(holder["policy"], field, replacements[field])
        return "order-1"

    result = FixedQuantityExecutionPolicy(
        quantity=1.0,
        price=None,
        clock=clock,
        order_id_factory=identifier,
        name="captured_name",
    )
    holder["policy"] = result
    return result, calls


@pytest.mark.parametrize("phase,field", CALLBACK_MUTATIONS)
def test_policy_rejects_valid_configuration_mutation_during_callbacks(
    phase: str, field: str
) -> None:
    configured, calls = mutating_policy(phase, field)
    with pytest.raises(
        ValueError,
        match="configuration must not change during create_intent",
    ):
        configured.create_intent(risk_for())
    assert calls["clock"] == 1
    assert calls["order_id_factory"] == (0 if phase == "clock" else 1)


@pytest.mark.parametrize("phase,field", CALLBACK_MUTATIONS)
def test_engine_does_not_publish_when_callback_mutates_valid_configuration(
    phase: str, field: str
) -> None:
    configured, calls = mutating_policy(phase, field)
    bus, events = EventBus(), []
    bus.subscribe(EXECUTION_UPDATED, events.append)
    with pytest.raises(
        ValueError,
        match="configuration must not change during create_intent",
    ):
        ExecutionEngine(bus, configured).create_intent(risk_for())
    assert calls["clock"] == 1
    assert calls["order_id_factory"] == (0 if phase == "clock" else 1)
    assert events == []


def test_policy_normal_output_uses_captured_configuration_once() -> None:
    calls = {"clock": 0, "order_id_factory": 0}

    def clock() -> datetime:
        calls["clock"] += 1
        return CREATED

    def identifier() -> str:
        calls["order_id_factory"] += 1
        return "captured-order"

    configured = FixedQuantityExecutionPolicy(
        quantity=3.0,
        price=61_000.0,
        clock=clock,
        order_id_factory=identifier,
        name="captured_policy",
    )
    intent = configured.create_intent(risk_for())
    assert calls == {"clock": 1, "order_id_factory": 1}
    assert intent.order.quantity == 3.0
    assert intent.order.price == 61_000.0
    assert intent.policy_name == "captured_policy"


@pytest.mark.parametrize("field,value", [
    ("quantity", 0), ("quantity", True), ("quantity", float("nan")),
    ("price", -1), ("price", "1"), ("clock", None),
    ("order_id_factory", None), ("name", " "),
])
def test_policy_rejects_invalid_configuration_and_runtime_tampering(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        policy(**{field: value})
    valid = policy()
    object.__setattr__(valid, field, value)
    with pytest.raises(ValueError):
        valid.create_intent(risk_for())


def test_policy_rejects_bad_input_rejected_risk_and_empty_identifier() -> None:
    with pytest.raises(TypeError, match="RiskAssessment"):
        policy().create_intent(None)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="APPROVED"):
        policy().create_intent(risk_for(outcome=RiskOutcome.REJECTED))
    with pytest.raises(ValueError, match="order_id"):
        policy(order_id_factory=lambda: " ").create_intent(risk_for())


def test_intent_fails_closed_after_order_or_intent_tampering() -> None:
    intent = policy().create_intent(risk_for())
    object.__setattr__(intent.order, "status", OrderStatus.SUBMITTED)
    with pytest.raises(ValueError, match="CREATED"):
        intent.validate()
    intent = policy().create_intent(risk_for())
    object.__setattr__(intent, "created_at", datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="timezone-aware"):
        intent.validate()


def test_intent_rejects_time_lineage_clone_and_wrong_side() -> None:
    risk = risk_for()
    valid = policy().create_intent(risk)
    for mutation in (
        lambda item: object.__setattr__(item, "symbol", "ETHUSDT"),
        lambda item: object.__setattr__(item, "source", "forged"),
        lambda item: object.__setattr__(item.order, "symbol", "ETHUSDT"),
        lambda item: object.__setattr__(item.order, "side", OrderSide.SELL),
        lambda item: object.__setattr__(item.order, "created_at", ASSESSED - timedelta(seconds=1)),
        lambda item: object.__setattr__(item, "created_at", ASSESSED - timedelta(seconds=1)),
    ):
        item = replace(valid)
        object.__setattr__(item, "order", replace(valid.order))
        mutation(item)
        with pytest.raises(ValueError):
            item.validate()


def test_engine_publishes_canonical_event_with_exact_identity_payload() -> None:
    bus, events, risk = EventBus(), [], risk_for()
    bus.subscribe(EXECUTION_UPDATED, events.append)
    result = ExecutionEngine(bus, policy(price=60_000.0)).create_intent(risk)
    assert EXECUTION_UPDATED is EventType.EXECUTION_UPDATED
    assert events[0].event_type is EventType.EXECUTION_UPDATED
    assert set(events[0].payload) == {"execution_intent", "order", "risk_assessment", "decision_intent", "alpha_candidate", "timing_assessment", "observation"}
    assert events[0].payload["execution_intent"] is result
    assert events[0].payload["order"] is result.order
    assert events[0].payload["risk_assessment"] is risk
    assert events[0].payload["decision_intent"] is risk.decision_intent
    assert events[0].payload["alpha_candidate"] is risk.decision_intent.alpha_candidate
    assert events[0].payload["timing_assessment"] is risk.decision_intent.alpha_candidate.timing_assessment
    assert events[0].payload["observation"] is risk.decision_intent.alpha_candidate.observation


def test_engine_default_policy_and_rejected_input() -> None:
    assert isinstance(ExecutionEngine(EventBus()).policy, FixedQuantityExecutionPolicy)
    bus, events = EventBus(), []
    bus.subscribe(EXECUTION_UPDATED, events.append)
    with pytest.raises(ValueError, match="APPROVED"):
        ExecutionEngine(bus).create_intent(risk_for(outcome=RiskOutcome.REJECTED))
    assert events == []


@pytest.mark.parametrize("mutation", [
    lambda risk: object.__setattr__(risk, "reasons", ("changed",)),
    lambda risk: object.__setattr__(risk, "policy_name", "changed"),
    lambda risk: object.__setattr__(risk.decision_intent, "reasons", ("changed",)),
    lambda risk: object.__setattr__(risk.decision_intent.alpha_candidate, "reasons", ("changed",)),
    lambda risk: object.__setattr__(risk, "decision_intent", replace(risk.decision_intent)),
    lambda risk: object.__setattr__(risk.decision_intent, "alpha_candidate", replace(risk.decision_intent.alpha_candidate)),
    lambda risk: object.__setattr__(risk.decision_intent.alpha_candidate, "timing_assessment", replace(risk.decision_intent.alpha_candidate.timing_assessment)),
    lambda risk: object.__setattr__(risk.decision_intent.alpha_candidate, "observation", replace(risk.decision_intent.alpha_candidate.observation)),
], ids=["risk-reasons", "risk-policy", "decision-reasons", "alpha-reasons", "decision-clone", "alpha-clone", "timing-clone", "observation-clone"])
def test_engine_rejects_upstream_mutation_without_event(mutation: object) -> None:
    bus, events, risk = EventBus(), [], risk_for()
    bus.subscribe(EXECUTION_UPDATED, events.append)
    class MutatingPolicy:
        name = "malicious"
        def create_intent(self, supplied: RiskAssessment) -> ExecutionIntent:
            mutation(supplied)  # type: ignore[operator]
            return policy(name=self.name).create_intent(supplied)
    with pytest.raises(ValueError):
        ExecutionEngine(bus, MutatingPolicy()).create_intent(risk)
    assert events == []


def test_engine_rejects_non_intent_rename_and_engine_policy_replacement_without_event() -> None:
    bus, events, risk = EventBus(), [], risk_for()
    bus.subscribe(EXECUTION_UPDATED, events.append)
    class Bad:
        name = "bad"
        def create_intent(self, supplied: RiskAssessment) -> object:
            return None
    with pytest.raises(TypeError):
        ExecutionEngine(bus, Bad()).create_intent(risk)  # type: ignore[arg-type]
    class Rename:
        name = "rename"
        def create_intent(self, supplied: RiskAssessment) -> ExecutionIntent:
            result = policy(name=self.name).create_intent(supplied)
            self.name = "changed"
            return result
    with pytest.raises(ValueError, match="must not change"):
        ExecutionEngine(bus, Rename()).create_intent(risk)
    class ReplaceEnginePolicy:
        name = "same"
        engine: ExecutionEngine
        def create_intent(self, supplied: RiskAssessment) -> ExecutionIntent:
            self.engine.policy = policy(name="same")
            return policy(name="same").create_intent(supplied)
    replacing = ReplaceEnginePolicy()
    engine = ExecutionEngine(bus, replacing)
    replacing.engine = engine
    with pytest.raises(ValueError, match="policy must not change"):
        engine.create_intent(risk)
    assert events == []


@pytest.mark.parametrize("mutation", [
    lambda intent: object.__setattr__(intent, "policy_name", "forged"),
    lambda intent: object.__setattr__(intent, "symbol", "ETHUSDT"),
    lambda intent: object.__setattr__(intent, "source", "forged"),
    lambda intent: object.__setattr__(intent.order, "symbol", "ETHUSDT"),
    lambda intent: object.__setattr__(intent.order, "side", OrderSide.SELL),
    lambda intent: object.__setattr__(intent.order, "quantity", 0),
    lambda intent: object.__setattr__(intent.order, "price", -1),
    lambda intent: object.__setattr__(intent.order, "order_id", ""),
    lambda intent: object.__setattr__(intent.order, "created_at", datetime(2026, 1, 1)),
    lambda intent: object.__setattr__(intent, "created_at", datetime(2026, 1, 1)),
    lambda intent: object.__setattr__(intent, "created_at", ASSESSED - timedelta(seconds=1)),
    lambda intent: object.__setattr__(intent.order, "created_at", ASSESSED - timedelta(seconds=1)),
    lambda intent: object.__setattr__(intent.order, "status", OrderStatus.SUBMITTED),
    lambda intent: object.__setattr__(intent.order, "status", OrderStatus.FILLED),
    lambda intent: object.__setattr__(intent.order, "status", OrderStatus.CANCELLED),
    lambda intent: object.__setattr__(intent.order, "status", OrderStatus.REJECTED),
], ids=[
    "policy-name", "symbol", "source", "order-symbol", "order-side", "quantity",
    "price", "order-id", "naive-order-time", "naive-intent-time", "early-intent-time",
    "early-order-time", "submitted", "filled", "cancelled", "rejected",
])
def test_engine_rejects_forged_output_without_event(mutation: object) -> None:
    bus, events, risk = EventBus(), [], risk_for()
    bus.subscribe(EXECUTION_UPDATED, events.append)
    class Forging:
        name = "forge"
        def create_intent(self, supplied: RiskAssessment) -> ExecutionIntent:
            intent = policy(name=self.name).create_intent(supplied)
            mutation(intent)  # type: ignore[operator]
            return intent
    with pytest.raises(ValueError):
        ExecutionEngine(bus, Forging()).create_intent(risk)
    assert events == []


def test_engine_rejects_equal_risk_clone_in_output_without_event() -> None:
    bus, events, risk = EventBus(), [], risk_for()
    bus.subscribe(EXECUTION_UPDATED, events.append)
    class Cloning:
        name = "clone"
        def create_intent(self, supplied: RiskAssessment) -> ExecutionIntent:
            return policy(name=self.name).create_intent(replace(supplied))
    with pytest.raises(ValueError, match="input RiskAssessment"):
        ExecutionEngine(bus, Cloning()).create_intent(risk)
    assert events == []
