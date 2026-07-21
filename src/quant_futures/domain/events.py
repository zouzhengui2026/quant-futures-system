"""Domain events.

All important system decisions will become auditable events.
"""

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class SystemEvent:
    event_type: str
    payload: dict
    timestamp: datetime = datetime.now(timezone.utc)
