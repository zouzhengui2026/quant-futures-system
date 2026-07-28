"""Injection protocol for decision policies."""

from typing import Protocol

from quant_futures.alpha.models import AlphaCandidate

from .models import DecisionIntent


class DecisionPolicy(Protocol):
    """Turns a candidate into a proposal or an abstention."""

    name: str

    def decide(self, candidate: AlphaCandidate) -> DecisionIntent:
        """Return an immutable decision intent for *candidate*."""
