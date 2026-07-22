"""Extension protocol for alpha-hypothesis models."""

from __future__ import annotations

from typing import Protocol

from quant_futures.timing.models import TimingAssessment

from .models import AlphaCandidate


class AlphaModel(Protocol):
    """Generate one explainable candidate from a timing assessment."""

    name: str

    def generate(self, timing: TimingAssessment) -> AlphaCandidate:
        """Return the model's hypothesis for *timing*."""
