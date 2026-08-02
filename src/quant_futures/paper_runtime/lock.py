"""Advisory single-writer lock scoped to a Paper Runtime directory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import IO


class RunLockError(RuntimeError):
    """Another writer owns the run directory or the lock cannot be opened."""


class RunDirectoryLock:
    """Hold an exclusive non-blocking OS lock for the duration of a mutation."""

    def __init__(self, run_directory: str | Path) -> None:
        self.path = Path(run_directory) / ".paper-runtime.lock"
        self._stream: IO[str] | None = None

    def acquire(self) -> "RunDirectoryLock":
        if self._stream is not None:
            raise RunLockError("run-directory lock is already held by this object")
        try:
            # Import the Unix backend only when Paper control is actually used.
            # This keeps the existing product CLI importable on platforms where
            # fcntl is unavailable and gives Paper control a deterministic error.
            try:
                import fcntl
            except ImportError as exc:
                raise RunLockError(
                    "Paper runtime locking is unsupported on this platform (fcntl unavailable)"
                ) from exc
            stream = self.path.open("a+", encoding="ascii")
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            stream.seek(0)
            stream.truncate()
            stream.write(f"pid={os.getpid()}\n")
            stream.flush()
        except (OSError, BlockingIOError) as exc:
            if 'stream' in locals():
                stream.close()
            raise RunLockError(f"run directory is already controlled by another writer: {self.path.parent}") from exc
        self._stream = stream
        return self

    def release(self) -> None:
        if self._stream is not None:
            import fcntl

            fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            self._stream.close()
            self._stream = None

    def __enter__(self) -> "RunDirectoryLock":
        return self.acquire()

    def __exit__(self, *_: object) -> None:
        self.release()


class RuntimeConsumerLease(RunDirectoryLock):
    """OS-released, non-blocking lease held by one replay consumer for its life.

    This is intentionally a different inode from the short transaction lock:
    controls may still take :class:`RunDirectoryLock` while a consumer is alive,
    but a second consumer (or recovery coordinator) cannot be constructed.
    ``flock`` releases the lease automatically when a process exits, including
    ungraceful death.
    """

    def __init__(self, run_directory: str | Path) -> None:
        self.path = Path(run_directory) / ".paper-runtime-consumer.lock"
        self._stream = None

    @classmethod
    def is_owned(cls, run_directory: str | Path) -> bool:
        probe = cls(run_directory)
        try:
            probe.acquire()
        except RunLockError:
            return True
        else:
            probe.release()
            return False
