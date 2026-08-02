"""Atomic authoritative checkpoints for fully committed Paper transitions."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Mapping

from .lifecycle import _fsync_directory
from .lock import RunDirectoryLock


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

    def write(self, value: Mapping[str, object]) -> bytes:
        """Lock and atomically replace the checkpoint authority."""
        with RunDirectoryLock(self.run_directory):
            return self._write_held(value)

    def _write_held(self, value: Mapping[str, object]) -> bytes:
        """Replace the authority while the enclosing runtime transaction holds the lock."""
        encoded = canonical_checkpoint(value)
        descriptor, name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.run_directory)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            _fsync_directory(self.run_directory)
        except BaseException as exc:
            temporary.unlink(missing_ok=True)
            # Once replace succeeds its outcome is deliberately not rolled back:
            # rewriting the authority in place could expose torn bytes.  A failed
            # directory fsync is an indeterminate (and therefore fail-closed)
            # publication, but the pathname still names one complete old/new file.
            if isinstance(exc, CheckpointError):
                raise
            raise CheckpointError(f"cannot persist checkpoint: {exc}") from exc
        return encoded
