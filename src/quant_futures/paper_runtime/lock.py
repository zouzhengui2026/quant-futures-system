"""Advisory single-writer lock scoped to a Paper Runtime directory."""

from __future__ import annotations

import os
from pathlib import Path
from typing import IO


_HELD_CONSUMER_CAPABILITY = object()


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
        self.run_directory = Path(run_directory).resolve()
        self.path = self.run_directory / ".paper-runtime-consumer.lock"
        self._stream = None
        self._capability: object | None = None

    def acquire(self) -> "RuntimeConsumerLease":
        super().acquire()
        self._capability = _HELD_CONSUMER_CAPABILITY
        return self

    def release(self) -> None:
        # Invalidate the capability before unlocking.  A runtime that is handed
        # this object cannot reuse it after ownership has been returned.
        self._capability = None
        super().release()

    def assert_held_for(self, run_directory: str | Path) -> None:
        """Prove this is the live capability for exactly ``run_directory``.

        Merely constructing a lease object is deliberately insufficient.  The
        private capability is installed only after a successful OS lock and is
        destroyed before release, while the open descriptor and canonical run
        directory are checked again at the consumer boundary.
        """
        expected = Path(run_directory).resolve()
        stream = self._stream
        if (self._capability is not _HELD_CONSUMER_CAPABILITY
                or stream is None or stream.closed
                or self.run_directory != expected
                or self.path != expected / ".paper-runtime-consumer.lock"):
            raise RunLockError("runtime consumer lease is not held for this run directory")
        try:
            os.fstat(stream.fileno())
        except (OSError, ValueError) as exc:
            raise RunLockError("runtime consumer lease descriptor is not live") from exc

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
