"""Atomic authoritative checkpoints for fully committed Paper transitions."""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Callable, Mapping

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
    _temporary_name = re.compile(r"^\.checkpoint\.json\.[^.]+\.tmp$")

    def __init__(self, run_directory: str | Path,
                 failure_injector: Callable[[str], None] | None = None) -> None:
        self.run_directory = Path(run_directory)
        self.path = self.run_directory / self.filename
        self._failure_injector = failure_injector or (lambda _boundary: None)

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
        self._remove_orphaned_temporaries_held()
        encoded = canonical_checkpoint(value)
        descriptor, name = tempfile.mkstemp(
            prefix=f".{self.path.name}.", suffix=".tmp", dir=self.run_directory)
        temporary = Path(name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                self._failure_injector("checkpoint_temporary_written")
                stream.flush()
                self._failure_injector("checkpoint_file_flush_completed")
                os.fsync(stream.fileno())
                self._failure_injector("checkpoint_file_fsync_completed")
            os.replace(temporary, self.path)
            self._failure_injector("checkpoint_atomic_replace_completed")
            self._failure_injector("checkpoint_replaced_before_directory_fsync")
            _fsync_directory(self.run_directory)
            self._failure_injector("checkpoint_directory_fsync_completed")
            self._failure_injector("checkpoint_post_directory_fsync_published")
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

    def _remove_orphaned_temporaries_held(self) -> None:
        """Remove only checkpoint temporaries while the run lock is held.

        A process cut cannot execute the writer's exception cleanup.  These
        files are never authority (only ``checkpoint.json`` is), so the next
        locked checkpoint transaction removes the exact mkstemp name pattern
        and makes those directory-entry deletions durable before proceeding.
        """
        removed = False
        try:
            entries = tuple(self.run_directory.iterdir())
        except OSError as exc:
            raise CheckpointError(f"cannot inspect checkpoint temporaries: {exc}") from exc
        for candidate in entries:
            if (self._temporary_name.fullmatch(candidate.name) is None
                    or not candidate.is_file()):
                continue
            try:
                candidate.unlink()
            except OSError as exc:
                raise CheckpointError(f"cannot remove orphaned checkpoint temporary: {exc}") from exc
            removed = True
        if removed:
            try:
                _fsync_directory(self.run_directory)
            except OSError as exc:
                raise CheckpointError(
                    f"cannot persist checkpoint temporary cleanup: {exc}") from exc
