"""Timing state transition rules.

The timing engine uses controlled lifecycle transitions instead of
instant state changes caused by a single market observation.
"""

from .state import TimingState


class TimingStateTransition:
    """Validate allowed opportunity lifecycle transitions."""

    _allowed = {
        TimingState.EARLY: {TimingState.CONFIRMED, TimingState.EXPIRED},
        TimingState.CONFIRMED: {TimingState.MATURE, TimingState.CROWDED, TimingState.EXPIRED},
        TimingState.MATURE: {TimingState.CROWDED, TimingState.EXHAUSTED, TimingState.EXPIRED},
        TimingState.CROWDED: {TimingState.EXHAUSTED, TimingState.EXPIRED},
        TimingState.EXHAUSTED: {TimingState.EXPIRED},
        TimingState.EXPIRED: set(),
    }

    def can_transition(self, current: TimingState, target: TimingState) -> bool:
        return target in self._allowed.get(current, set())
