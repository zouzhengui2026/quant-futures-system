from dataclasses import dataclass


@dataclass
class ObservationConfidence:
    """Measures how reliable the current market observation is."""

    overall: float
    data_quality: float
    regime_confidence: float
    flow_confidence: float

    def is_reliable(self, threshold: float = 50.0) -> bool:
        return self.overall >= threshold


def calculate_observation_confidence(
    data_quality: float,
    regime_confidence: float,
    flow_confidence: float,
) -> ObservationConfidence:
    """Calculate market understanding confidence.

    This does not predict profitability. It measures whether the system
    has sufficient understanding of the current market state.
    """

    overall = (
        data_quality * 0.4
        + regime_confidence * 0.35
        + flow_confidence * 0.25
    )

    return ObservationConfidence(
        overall=overall,
        data_quality=data_quality,
        regime_confidence=regime_confidence,
        flow_confidence=flow_confidence,
    )
