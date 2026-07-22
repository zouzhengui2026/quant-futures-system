"""Backward-compatible domain event imports.

Runtime events are defined in :mod:`quant_futures.core.events` so all layers
share one transport-neutral event contract.
"""

from quant_futures.core.events import Event

SystemEvent = Event

__all__ = ["Event", "SystemEvent"]
