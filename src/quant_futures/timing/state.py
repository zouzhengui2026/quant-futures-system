from enum import Enum


class TimingState(str, Enum):
    """Lifecycle stage of a potential opportunity."""

    EARLY = "early"
    CONFIRMED = "confirmed"
    MATURE = "mature"
    CROWDED = "crowded"
    EXHAUSTED = "exhausted"
    EXPIRED = "expired"


class TimingResult:
    def __init__(self, state: TimingState, confidence: float, reason: str = ""):
        self.state = state
        self.confidence = confidence
        self.reason = reason
