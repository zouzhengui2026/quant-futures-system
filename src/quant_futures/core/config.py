"""Central configuration primitives.

All runtime configuration must be explicit and validated.
No trading module may bypass configuration controls.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SystemConfig:
    environment: str = "development"
    trading_mode: str = "paper"
    max_leverage: float = 3.0
    max_positions: int = 3

    def is_live(self) -> bool:
        return self.trading_mode == "live"
