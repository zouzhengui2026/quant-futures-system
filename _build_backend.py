"""Small, dependency-free PEP 517 wheel backend for this pure-Python project."""
from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path

NAME = "quant_futures_system"
VERSION = "0.1.0"
DIST_INFO = f"{NAME}-{VERSION}.dist-info"


def _record_digest(data: bytes) -> str:
    value = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=")
    return f"sha256={value.decode('ascii')}"


def build_wheel(wheel_directory: str, config_settings=None, metadata_directory=None) -> str:
    """Build a deterministic, portable wheel without downloading build tools."""
    del config_settings, metadata_directory
    root = Path(__file__).parent
    members: dict[str, bytes] = {}
    for source in sorted((root / "src" / "quant_futures").rglob("*.py")):
        members[source.relative_to(root / "src").as_posix()] = source.read_bytes()
    members[f"{DIST_INFO}/METADATA"] = (
        "Metadata-Version: 2.1\nName: quant-futures-system\nVersion: 0.1.0\n"
        "Summary: Crypto perpetual futures quantitative trading system\n"
        "Requires-Python: >=3.11\n\n"
    ).encode()
    members[f"{DIST_INFO}/WHEEL"] = (
        "Wheel-Version: 1.0\nGenerator: quant-futures-system\n"
        "Root-Is-Purelib: true\nTag: py3-none-any\n"
    ).encode()
    members[f"{DIST_INFO}/entry_points.txt"] = (
        "[console_scripts]\nquant-futures = quant_futures.product.cli:main\n"
    ).encode()
    rows = [[name, _record_digest(data), str(len(data))] for name, data in members.items()]
    rows.append([f"{DIST_INFO}/RECORD", "", ""])
    output = io.StringIO(newline="")
    csv.writer(output, lineterminator="\n").writerows(rows)
    members[f"{DIST_INFO}/RECORD"] = output.getvalue().encode()
    filename = f"{NAME}-{VERSION}-py3-none-any.whl"
    destination = Path(wheel_directory) / filename
    destination.parent.mkdir(parents=True, exist_ok=True)
    timestamp = (2020, 1, 1, 0, 0, 0)
    with zipfile.ZipFile(destination, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, data in members.items():
            info = zipfile.ZipInfo(name, timestamp)
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, data)
    return filename
