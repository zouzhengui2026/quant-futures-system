"""Deprecated compatibility module; canonical observation models live in ``models``.

This module defines no models and only re-exports ``MarketObservation`` for
existing callers. New code must import from :mod:`quant_futures.observation.models`.
"""

from quant_futures.observation.models import MarketObservation

__all__ = ["MarketObservation"]
