"""Run identity and collision-safe directory operations."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any


def run_id(config: dict[str, Any], data_fingerprint: str, strategy_version: str,
           version: str = "0.1.0") -> str:
    payload = json.dumps({"config": config, "data": data_fingerprint,
                          "strategy": strategy_version, "version": version},
                         sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def create_run_directory(root: str | Path, identifier: str) -> Path:
    path = Path(root) / identifier
    try:
        path.mkdir(parents=True, exist_ok=False)
    except FileExistsError as exc:
        raise FileExistsError(f"run already exists; refusing overwrite: {path}") from exc
    (path / ".lock").write_text(f"pid={os.getpid()}\n", encoding="ascii")
    return path


def atomic_write(path: Path, content: str) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)
