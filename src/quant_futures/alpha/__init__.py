"""Explainable alpha-hypothesis generation layer.

This package expresses directional market views only; decision, risk, and
execution responsibilities belong to later layers.
"""

from .baseline import RegimeDirectionalAlphaModel
from .engine import ALPHA_GENERATED, AlphaEngine
from .models import AlphaCandidate, AlphaDirection
from .protocols import AlphaModel

__all__ = [
    "ALPHA_GENERATED",
    "AlphaCandidate",
    "AlphaDirection",
    "AlphaEngine",
    "AlphaModel",
    "RegimeDirectionalAlphaModel",
]
