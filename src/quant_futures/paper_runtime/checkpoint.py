"""Atomic authoritative checkpoints for fully committed Paper transitions."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Mapping

from .lifecycle import _fsync_directory


class CheckpointError(ValueError):
    """A checkpoint is malformed, inconsistent, or could not be persisted."""


def canonical_checkpoint(value: Mapping[str, object]) -> bytes:
    try:
        return (json.dumps(value, sort_keys=True, separators=(",", ":"),
                           ensure_ascii=True, allow_nan=False) + "\n").encode("ascii")
    except (TypeError, ValueError) as exc:
        raise CheckpointError("checkpoint must contain canonical JSON values") from exc


class CheckpointStore:
    """Versioned checkpoint authority using durable same-directory replacement."""

    filename = "checkpoint.json"

    def __init__(self, run_directory: str | Path) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename

    def read(self) -> dict[str, object]:
        try:
            raw = self.path.read_bytes()
            value = json.loads(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CheckpointError(f"cannot read checkpoint: {exc}") from exc
        if not isinstance(value, dict) or canonical_checkpoint(value) != raw:
            raise CheckpointError("checkpoint is not a canonical object")
        return value

    def write_held(self, value: Mapping[str, object]) -> bytes:
        """Replace the authority; the caller must hold ``RunDirectoryLock``."""
        encoded = canonical_checkpoint(value)
        previous = self.path.read_bytes() if self.path.exists() else None
        descriptor, name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.run_directory)
        temporary = Path(name)
        replaced = False
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            replaced = True
            _fsync_directory(self.run_directory)
        except BaseException as exc:
            temporary.unlink(missing_ok=True)
            if replaced:
                try:
                    if previous is None:
                        self.path.unlink(missing_ok=True)
                    else:
                        self.path.write_bytes(previous)
                        with self.path.open("rb") as stream:
                            os.fsync(stream.fileno())
                    _fsync_directory(self.run_directory)
                except OSError:
                    pass
            if isinstance(exc, CheckpointError):
                raise
            raise CheckpointError(f"cannot persist checkpoint: {exc}") from exc
        return encoded
