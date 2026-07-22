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
    assert VolatilityConditionRule().evaluate(item).outcome is RuleOutcome.FAIL
    assert LiquidityConditionRule().evaluate(item).outcome is RuleOutcome.FAIL


def test_engine_assesses_environment_and_publishes_timing_created_event() -> None:
    bus = EventBus()
    received = []
    bus.subscribe(EventType.TIMING_CREATED, received.append)
    engine = TimingEngine(bus, clock=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))

    assessment = engine.assess(observation())

    assert assessment.status is TimingStatus.FAVORABLE
    assert assessment.assessed_at == datetime(2026, 1, 2, tzinfo=timezone.utc)
    assert received[0].payload["assessment"] == assessment
    assert received[0].payload["observation"] == assessment.observation


@pytest.mark.parametrize("invalid", [None, object(), "not an observation"])
def test_engine_rejects_invalid_input(invalid: object) -> None:
    with pytest.raises(TypeError, match="MarketObservation"):
        TimingEngine(EventBus()).assess(invalid)  # type: ignore[arg-type]


def test_engine_waits_for_unconfirmed_conditions_and_rejects_unsafe_ones() -> None:
    engine = TimingEngine(EventBus())

    waiting = engine.assess(observation(TrendRegime.RANGE, VolatilityRegime.LOW, LiquidityRegime.THIN))
    unfavorable = engine.assess(observation(volatility=VolatilityRegime.HIGH))

    assert waiting.status is TimingStatus.WAITING
    assert unfavorable.status is TimingStatus.UNFAVORABLE


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("status", "favorable", "TimingStatus"),
        ("evaluations", [], "non-empty tuple"),
        ("assessed_at", datetime(2026, 1, 1), "timezone-aware"),
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
