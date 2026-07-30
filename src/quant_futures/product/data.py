"""Strict, future-blind OHLCV catalog."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import csv, hashlib, math
from pathlib import Path

@dataclass(frozen=True, slots=True)
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    # ``None`` means this row is not a funding event.  A numeric value,
    # including zero, means that the dataset explicitly supplied an event.
    funding_rate: float | None = None

def load_bars(path: str | Path, schema: dict[str, str], *, start: str | None = None,
              end: str | None = None, timeframe: str | None = None) -> tuple[tuple[Bar, ...], str]:
    source = Path(path)
    if source.suffix.lower() != ".csv":
        raise ValueError("only CSV is available in the dependency-free installation")
    payload = source.read_bytes()
    rows: list[Bar] = []
    with source.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        required = tuple(schema[k] for k in ("timestamp", "open", "high", "low", "close", "volume"))
        if not reader.fieldnames or any(name not in reader.fieldnames for name in required):
            raise ValueError("CSV does not contain the configured OHLCV schema")
        for number, raw in enumerate(reader, 2):
            try:
                timestamp = datetime.fromisoformat(raw[schema["timestamp"]].replace("Z", "+00:00"))
                values = [float(raw[schema[k]]) for k in ("open", "high", "low", "close", "volume")]
                funding_name = schema.get("funding_rate", "funding_rate")
                funding_raw = raw.get(funding_name)
                funding = None if funding_raw is None or not funding_raw.strip() else float(funding_raw)
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError(f"malformed CSV row {number}") from exc
            o, h, l, c, v = values
            if timestamp.tzinfo is None or timestamp.utcoffset() != timezone.utc.utcoffset(timestamp):
                raise ValueError(f"row {number} timestamp is not UTC-aware")
            if not all(math.isfinite(x) for x in values) or (funding is not None and
                    not math.isfinite(funding)) or v < 0:
                raise ValueError(f"row {number} contains non-finite values or negative volume")
            if min(o, c) < l or max(o, c) > h or l > h or l <= 0:
                raise ValueError(f"row {number} has invalid OHLC values")
            rows.append(Bar(timestamp, o, h, l, c, v, funding))
    if not rows or any(a.timestamp >= b.timestamp for a, b in zip(rows, rows[1:])):
        raise ValueError("bars must be non-empty, unique, and strictly ordered")
    lower = datetime.fromisoformat(start.replace("Z", "+00:00")) if start else None
    upper = datetime.fromisoformat(end.replace("Z", "+00:00")) if end else None
    rows = [bar for bar in rows if (lower is None or bar.timestamp >= lower) and
            (upper is None or bar.timestamp <= upper)]
    if not rows:
        raise ValueError("configured date range contains no bars")
    if timeframe:
        units = {"m": "minutes", "h": "hours", "d": "days"}
        try:
            amount, unit = int(timeframe[:-1]), timeframe[-1]
            expected = timedelta(**{units[unit]: amount})
        except (ValueError, KeyError):
            raise ValueError("timeframe must use a positive integer followed by m, h, or d") from None
        if amount <= 0 or any(b.timestamp - a.timestamp != expected for a, b in zip(rows, rows[1:])):
            raise ValueError("bars violate the configured strict timeframe/gap policy")
    return tuple(rows), hashlib.sha256(payload).hexdigest()

def replay(bars: tuple[Bar, ...]):
    """Yield one immutable bar at a time; consumers cannot access the collection."""
    yield from bars
