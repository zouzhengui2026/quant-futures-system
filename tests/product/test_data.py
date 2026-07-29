from pathlib import Path
import pytest
from quant_futures.product.data import load_bars, replay

SCHEMA = {k:k for k in ("timestamp", "open", "high", "low", "close", "volume")}
def test_strict_loader(tmp_path: Path):
    p=tmp_path/"x.csv"; p.write_text("timestamp,open,high,low,close,volume\n2024-01-01T00:00:00Z,1,2,1,2,3\n")
    bars, fingerprint=load_bars(p, SCHEMA)
    assert tuple(replay(bars)) == bars and len(fingerprint)==64
def test_rejects_invalid_ohlc(tmp_path: Path):
    p=tmp_path/"x.csv"; p.write_text("timestamp,open,high,low,close,volume\n2024-01-01T00:00:00Z,3,2,1,2,3\n")
    with pytest.raises(ValueError): load_bars(p, SCHEMA)
