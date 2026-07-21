class TimingConfidence:
    """Confidence score for timing evaluation, not trade probability."""

    def __init__(self, score: float):
        self.score = max(0.0, min(100.0, score))
