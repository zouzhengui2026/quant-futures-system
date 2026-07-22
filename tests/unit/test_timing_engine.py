from datetime import datetime, timezone

import pytest

from quant_futures.core.events import EventBus, EventType
from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.models import (
    LiquidityRegime,
    MarketObservation,
    TrendRegime,
    VolatilityRegime,
)
from quant_futures.timing import (
    LiquidityConditionRule,
    RuleOutcome,
    TimingAssessment,
    TimingEngine,
    TimingStatus,
    TrendAlignmentRule,
    VolatilityConditionRule,
)


def observation(
    trend: TrendRegime = TrendRegime.UPTREND,
    volatility: VolatilityRegime = VolatilityRegime.NORMAL,
    liquidity: LiquidityRegime = LiquidityRegime.LIQUID,
) -> MarketObservation:
    return MarketObservation(
        symbol="BTCUSDT",
        source="replay",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        price=60_000.0,
        trend_regime=trend,
        volatility_regime=volatility,
        liquidity_regime=liquidity,
        features=MarketFeatures(),
    )


def test_timing_assessment_exposes_status_and_rule_reasons() -> None:
    item = observation()
    assessment = TimingAssessment(
        observation=item,
        status=TimingStatus.FAVORABLE,
        evaluations=(TrendAlignmentRule().evaluate(item),),
        assessed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert assessment.state is TimingStatus.FAVORABLE
    assert assessment.reasons == ("market has a directional trend regime",)


def test_basic_rules_evaluate_trend_volatility_and_liquidity() -> None:
    item = observation(TrendRegime.RANGE, VolatilityRegime.HIGH, LiquidityRegime.STRESSED)

    assert TrendAlignmentRule().evaluate(item).outcome is RuleOutcome.NEUTRAL
    assert VolatilityConditionRule().evaluate(item).outcome is RuleOutcome.NEUTRAL
    assert LiquidityConditionRule().evaluate(item).outcome is RuleOutcome.FAIL


def test_engine_assesses_environment_and_publishes_timing_created_event() -> None:
    bus = EventBus()
    received = []
    bus.subscribe(EventType.TIMING_CREATED, received.append)
    engine = TimingEngine(bus, clock=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))

    assessment = engine.assess(observation())

    assert assessment.status is TimingStatus.FAVORABLE
    assert assessment.assessed_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert assessment.confidence == pytest.approx(1.0)
    assert received[0].payload["assessment"] == assessment
    assert received[0].payload["observation"] == assessment.observation


@pytest.mark.parametrize("invalid", [None, object(), "not an observation"])
def test_engine_rejects_invalid_input(invalid: object) -> None:
    with pytest.raises(TypeError, match="MarketObservation"):
        TimingEngine(EventBus()).assess(invalid)  # type: ignore[arg-type]


def test_engine_waits_for_unconfirmed_conditions_and_rejects_unsafe_ones() -> None:
    engine = TimingEngine(EventBus())

    waiting = engine.assess(observation(TrendRegime.RANGE, VolatilityRegime.LOW, LiquidityRegime.THIN))
    high_volatility = engine.assess(observation(volatility=VolatilityRegime.HIGH))
    stressed = engine.assess(observation(liquidity=LiquidityRegime.STRESSED))

    assert waiting.status is TimingStatus.WAITING
    assert high_volatility.status is TimingStatus.WATCHING
    assert stressed.status is TimingStatus.UNFAVORABLE


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("status", "favorable", "TimingStatus"),
        ("evaluations", [], "non-empty tuple"),
        ("assessed_at", datetime(2026, 1, 1), "timezone-aware"),
        ("confidence", -0.01, "between 0.0 and 1.0"),
        ("confidence", 1.01, "between 0.0 and 1.0"),
        ("confidence", float("nan"), "between 0.0 and 1.0"),
    ],
)
def test_timing_assessment_rejects_invalid_fields(field: str, value: object, message: str) -> None:
    item = observation()
    fields: dict[str, object] = {
        "observation": item,
        "status": TimingStatus.FAVORABLE,
        "evaluations": (TrendAlignmentRule().evaluate(item),),
        "assessed_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }
    fields[field] = value

    with pytest.raises(ValueError, match=message):
        TimingAssessment(**fields)  # type: ignore[arg-type]


@pytest.mark.parametrize("rule", [TrendAlignmentRule(), VolatilityConditionRule(), LiquidityConditionRule()])
def test_rules_reject_invalid_observation(rule: object) -> None:
    with pytest.raises(TypeError, match="MarketObservation"):
        rule.evaluate(None)  # type: ignore[union-attr]


@pytest.mark.parametrize("confidence", [0.0, 1.0])
def test_timing_assessment_accepts_confidence_boundaries(confidence: float) -> None:
    item = observation()

    assessment = TimingAssessment(
        observation=item,
        status=TimingStatus.FAVORABLE,
        evaluations=(TrendAlignmentRule().evaluate(item),),
        assessed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        confidence=confidence,
    )

    assert assessment.confidence == confidence


def test_engine_confidence_is_deterministic_from_rule_outcomes() -> None:
    engine = TimingEngine(EventBus())

    favorable = engine.assess(observation())
    neutral_environment = engine.assess(observation(TrendRegime.RANGE, VolatilityRegime.HIGH))
    stressed = engine.assess(observation(liquidity=LiquidityRegime.STRESSED))

    assert favorable.confidence == pytest.approx(1.0)
    assert neutral_environment.confidence == pytest.approx(2 / 3)
    assert stressed.confidence == pytest.approx(2 / 3)


def test_range_market_does_not_block_downstream_alpha() -> None:
    assessment = TimingEngine(EventBus()).assess(observation(trend=TrendRegime.RANGE))

    assert assessment.status is TimingStatus.WATCHING
    assert assessment.confidence == pytest.approx(5 / 6)


def test_high_volatility_with_normal_liquidity_is_not_unfavorable() -> None:
    assessment = TimingEngine(EventBus()).assess(observation(volatility=VolatilityRegime.HIGH))

    assert assessment.status is TimingStatus.WATCHING
    assert all(evaluation.outcome is not RuleOutcome.FAIL for evaluation in assessment.evaluations)


def test_stressed_liquidity_remains_a_blocking_safety_condition() -> None:
    assessment = TimingEngine(EventBus()).assess(observation(liquidity=LiquidityRegime.STRESSED))

    assert assessment.status is TimingStatus.UNFAVORABLE
    assert next(item for item in assessment.evaluations if item.rule == "liquidity_condition").outcome is RuleOutcome.FAIL
