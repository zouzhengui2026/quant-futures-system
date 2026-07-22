from dataclasses import fields
from datetime import datetime, timezone

import pytest

from quant_futures.alpha import AlphaCandidate, AlphaDirection, AlphaEngine, RegimeDirectionalAlphaModel
from quant_futures.core.events import EventBus, EventType
from quant_futures.observation.features import MarketFeatures
from quant_futures.observation.models import LiquidityRegime, MarketObservation, TrendRegime, VolatilityRegime
from quant_futures.timing import RuleEvaluation, RuleOutcome, TimingAssessment, TimingStatus


def observation(trend: TrendRegime = TrendRegime.UPTREND, strength: float = 0.6) -> MarketObservation:
    return MarketObservation(
        symbol="BTCUSDT",
        source="replay",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        price=60_000.0,
        trend_regime=trend,
        volatility_regime=VolatilityRegime.NORMAL,
        liquidity_regime=LiquidityRegime.LIQUID,
        features=MarketFeatures(trend_strength=strength),
    )


def timing(
    trend: TrendRegime = TrendRegime.UPTREND,
    status: TimingStatus = TimingStatus.FAVORABLE,
    confidence: float = 0.8,
    strength: float = 0.6,
) -> TimingAssessment:
    item = observation(trend, strength)
    return TimingAssessment(
        observation=item,
        status=status,
        evaluations=(RuleEvaluation("test", RuleOutcome.PASS, "test environment"),),
        assessed_at=datetime(2026, 1, 1, 1, tzinfo=timezone.utc),
        confidence=confidence,
    )


def candidate_for(assessment: TimingAssessment, **overrides: object) -> AlphaCandidate:
    item = assessment.observation
    fields = {
        "symbol": item.symbol,
        "source": item.source,
        "observation_timestamp": item.timestamp,
        "generated_at": datetime(2026, 1, 1, 2, tzinfo=timezone.utc),
        "model_name": "test_model",
        "direction": AlphaDirection.LONG,
        "strength": 0.5,
        "confidence": 0.6,
        "reasons": ("test reason",),
        "observation": item,
        "timing_assessment": assessment,
    }
    fields.update(overrides)
    return AlphaCandidate(**fields)  # type: ignore[arg-type]


def test_alpha_candidate_constructs_immutable_explainable_hypothesis() -> None:
    candidate = candidate_for(timing())

    assert candidate.direction is AlphaDirection.LONG
    assert candidate.reasons == ("test reason",)
    with pytest.raises(AttributeError):
        candidate.strength = 0.1  # type: ignore[misc]


@pytest.mark.parametrize("field,value", [("strength", -0.1), ("strength", 1.1), ("confidence", float("nan"))])
def test_alpha_candidate_rejects_invalid_scores(field: str, value: float) -> None:
    with pytest.raises(ValueError, match="between 0.0 and 1.0"):
        candidate_for(timing(), **{field: value})


def test_alpha_candidate_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        candidate_for(timing(), generated_at=datetime(2026, 1, 1))


def test_alpha_candidate_rejects_mismatched_timing_observation() -> None:
    assessment = timing()
    with pytest.raises(ValueError, match="match timing_assessment.observation"):
        candidate_for(assessment, observation=observation(TrendRegime.DOWNTREND))


@pytest.mark.parametrize(
    ("trend", "direction"),
    [(TrendRegime.UPTREND, AlphaDirection.LONG), (TrendRegime.DOWNTREND, AlphaDirection.SHORT), (TrendRegime.RANGE, AlphaDirection.NEUTRAL)],
)
def test_baseline_generates_direction_from_regime(trend: TrendRegime, direction: AlphaDirection) -> None:
    model = RegimeDirectionalAlphaModel(clock=lambda: datetime(2026, 1, 2, tzinfo=timezone.utc))

    candidate = model.generate(timing(trend))

    assert candidate.direction is direction
    assert candidate.generated_at == datetime(2026, 1, 2, tzinfo=timezone.utc)


def test_unfavorable_timing_forces_neutral_zero_alpha() -> None:
    candidate = RegimeDirectionalAlphaModel().generate(timing(status=TimingStatus.UNFAVORABLE))

    assert candidate.direction is AlphaDirection.NEUTRAL
    assert candidate.strength == candidate.confidence == 0.0
    assert "safety condition" in candidate.reasons[0]


def test_baseline_strength_and_confidence_are_deterministic_and_bounded() -> None:
    assessment = timing(confidence=0.8, strength=0.6)
    model = RegimeDirectionalAlphaModel()

    first = model.generate(assessment)
    second = model.generate(assessment)

    assert first.strength == second.strength == pytest.approx(0.48)
    assert first.confidence == second.confidence == pytest.approx(0.8)
    assert 0.0 <= first.strength <= 1.0
    assert 0.0 <= first.confidence <= 1.0


def test_engine_publishes_alpha_generated_event() -> None:
    bus = EventBus()
    received = []
    bus.subscribe(EventType.ALPHA_GENERATED, received.append)
    assessment = timing()

    candidate = AlphaEngine(bus).generate(assessment)

    assert received[0].payload == {
        "candidate": candidate,
        "timing_assessment": assessment,
        "observation": assessment.observation,
    }


def test_engine_rejects_invalid_input() -> None:
    with pytest.raises(TypeError, match="TimingAssessment"):
        AlphaEngine(EventBus()).generate(None)  # type: ignore[arg-type]


def test_engine_accepts_injected_alpha_model() -> None:
    assessment = timing()

    class CustomModel:
        name = "custom"

        def generate(self, timing: TimingAssessment) -> AlphaCandidate:
            return candidate_for(timing, model_name=self.name, direction=AlphaDirection.NEUTRAL)

    candidate = AlphaEngine(EventBus(), model=CustomModel()).generate(assessment)

    assert candidate.model_name == "custom"
    assert candidate.direction is AlphaDirection.NEUTRAL


def test_alpha_candidate_has_no_decision_risk_or_execution_fields() -> None:
    names = {item.name for item in fields(AlphaCandidate)}

    assert not names.intersection({"order", "quantity", "leverage", "stop_loss", "position", "risk_limit", "account_balance"})
