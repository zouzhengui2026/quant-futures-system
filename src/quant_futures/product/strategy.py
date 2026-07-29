"""Stable future-blind strategy API and built-in deterministic strategies."""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol
from .data import Bar

@dataclass(frozen=True, slots=True)
class StrategyContext:
    bar: Bar
    closes: tuple[float, ...]
    current_position: float

class Strategy(Protocol):
    name: str
    version: str
    def target(self, context: StrategyContext) -> float: ...

@dataclass(slots=True)
class MovingAverageCrossover:
    fast: int
    slow: int
    name: str = "moving_average_crossover"
    version: str = "1"
    def __post_init__(self):
        if self.fast < 1 or self.slow <= self.fast: raise ValueError("require 0 < fast < slow")
    def target(self, context: StrategyContext) -> float:
        if len(context.closes) < self.slow: return 0.0
        fast=sum(context.closes[-self.fast:])/self.fast; slow=sum(context.closes[-self.slow:])/self.slow
        return 1.0 if fast > slow else -1.0 if fast < slow else 0.0

@dataclass(slots=True)
class ChannelBreakout:
    lookback: int
    name: str = "channel_breakout"
    version: str = "1"
    def __post_init__(self):
        if self.lookback < 2: raise ValueError("lookback must be at least 2")
    def target(self, context: StrategyContext) -> float:
        prior=context.closes[-self.lookback-1:-1]
        if len(prior)<self.lookback: return 0.0
        return 1.0 if context.bar.close>max(prior) else -1.0 if context.bar.close<min(prior) else context.current_position

@dataclass(slots=True)
class FixedStrategy:
    value: float
    name: str = "fixed"
    version: str = "1"
    def __post_init__(self):
        if self.value not in {-1.0, 0.0, 1.0}: raise ValueError("fixed value must be -1, 0, or 1")
    def target(self, context: StrategyContext) -> float: return self.value

def build_strategy(name: str, parameters: dict) -> Strategy:
    if name == "moving_average_crossover": return MovingAverageCrossover(**parameters)
    if name == "channel_breakout": return ChannelBreakout(**parameters)
    if name in {"hold", "flat"}: return FixedStrategy(1.0 if name=="hold" else 0.0, name=name)
    raise ValueError(f"unknown strategy: {name}")
