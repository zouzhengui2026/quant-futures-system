"""Strict, future-blind OHLCV catalog."""
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
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
    funding_rate: float = 0.0

def load_bars(path: str | Path, schema: dict[str, str]) -> tuple[tuple[Bar, ...], str]:
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
                funding = float(raw.get(schema.get("funding_rate", "funding_rate"), 0) or 0)
            except (ValueError, TypeError, KeyError) as exc:
                raise ValueError(f"malformed CSV row {number}") from exc
            o, h, l, c, v = values
            if timestamp.tzinfo is None or timestamp.utcoffset() != timezone.utc.utcoffset(timestamp):
                raise ValueError(f"row {number} timestamp is not UTC-aware")
            if not all(math.isfinite(x) for x in (*values, funding)) or v < 0:
                raise ValueError(f"row {number} contains non-finite values or negative volume")
            if min(o, c) < l or max(o, c) > h or l > h or l <= 0:
                raise ValueError(f"row {number} has invalid OHLC values")
            rows.append(Bar(timestamp, o, h, l, c, v, funding))
    if not rows or any(a.timestamp >= b.timestamp for a, b in zip(rows, rows[1:])):
        raise ValueError("bars must be non-empty, unique, and strictly ordered")
    return tuple(rows), hashlib.sha256(payload).hexdigest()

def replay(bars: tuple[Bar, ...]):
    """Yield one immutable bar at a time; consumers cannot access the collection."""
    yield from bars
