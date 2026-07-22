"""Orchestration for generating and publishing alpha candidates."""

from __future__ import annotations

from dataclasses import dataclass, field

from quant_futures.core.events import Event, EventBus, EventType
from quant_futures.core.exceptions import DomainValidationError
from quant_futures.timing.models import TimingAssessment

from .baseline import RegimeDirectionalAlphaModel
from .models import AlphaCandidate
from .protocols import AlphaModel

ALPHA_GENERATED = EventType.ALPHA_GENERATED


@dataclass(slots=True)
class AlphaEngine:
    """Generate one validated candidate without making any trading decision."""

    event_bus: EventBus
    model: AlphaModel = field(default_factory=RegimeDirectionalAlphaModel)

    def generate(self, timing: TimingAssessment) -> AlphaCandidate:
        """Generate and publish an alpha candidate for a timing assessment."""
        if not isinstance(timing, TimingAssessment):
            raise TypeError("generate expects a TimingAssessment")
        model_name = getattr(self.model, "name", None)
        if not isinstance(model_name, str) or not model_name.strip():
            raise DomainValidationError("AlphaModel.name must be a non-empty string")

        candidate = self.model.generate(timing)
        if not isinstance(candidate, AlphaCandidate):
            raise TypeError("AlphaModel.generate must return an AlphaCandidate")
        if candidate.timing_assessment != timing or candidate.observation != timing.observation:
            raise ValueError("AlphaModel output must reference the input timing assessment and observation")
        if candidate.model_name != model_name:
            raise ValueError("AlphaModel output model_name must match AlphaModel.name")

        self.event_bus.publish(
            Event(ALPHA_GENERATED, {"candidate": candidate, "timing_assessment": timing, "observation": timing.observation})
        )
        return candidate
