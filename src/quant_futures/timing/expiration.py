from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class SignalExpiration:
    created_at: datetime
    expires_at: datetime
    reason: str = ""

    def is_expired(self, now: datetime) -> bool:
        return now >= self.expires_at
