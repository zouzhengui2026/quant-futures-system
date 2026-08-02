"""Physical persistence-boundary evidence using disposable child processes."""

from __future__ import annotations

import multiprocessing
import os

import pytest

from quant_futures.paper_runtime.checkpoint import CheckpointStore, canonical_checkpoint
from quant_futures.paper_runtime.journal import TransitionJournal


def _crash_journal(directory: str, boundary: str) -> None:
    def cut(name: str) -> None:
        if name == boundary:
            os._exit(91)

    TransitionJournal(directory, cut).append(
        "run", "transition", "transition_started", "bar",
        "2025-01-01T00:00:00+00:00", 0, {"value": 1},
    )


@pytest.mark.parametrize("boundary", [
    "journal_partial_frame_written",
    "journal_frame_write_completed",
    "journal_before_flush",
    "journal_flush_completed",
    "journal_fsync_completed",
    "journal_post_fsync_published",
])
def test_child_exit_at_physical_journal_boundary_is_repairable_or_committed(
    tmp_path, boundary,
):
    process = multiprocessing.Process(
        target=_crash_journal, args=(str(tmp_path), boundary))
    process.start(); process.join(10)
    assert process.exitcode == 91

    journal = TransitionJournal(tmp_path)
    with open(tmp_path / ".paper-runtime.lock", "a+b"):
        # Public append performs the lock-scoped scan/repair before publication.
        journal.append("run", "continuation", "transition_started", "bar",
                       "2025-01-01T01:00:00+00:00", 1, {"value": 2})
    records = journal.records()
    assert records[-1].product_transition_id == "continuation"
    assert len(records) in {1, 2}
    assert [record.sequence for record in records] == list(range(1, len(records) + 1))


def _crash_checkpoint(directory: str, boundary: str) -> None:
    def cut(name: str) -> None:
        if name == boundary:
            os._exit(92)

    CheckpointStore(directory, cut).write({"schema_version": 1, "value": "new"})


@pytest.mark.parametrize("boundary", [
    "checkpoint_temporary_written",
    "checkpoint_file_flush_completed",
    "checkpoint_file_fsync_completed",
    "checkpoint_atomic_replace_completed",
    "checkpoint_directory_fsync_completed",
    "checkpoint_post_directory_fsync_published",
])
def test_child_exit_at_physical_checkpoint_boundary_leaves_exact_old_or_new(
    tmp_path, boundary,
):
    store = CheckpointStore(tmp_path)
    old = store.write({"schema_version": 1, "value": "old"})
    new = canonical_checkpoint({"schema_version": 1, "value": "new"})
    process = multiprocessing.Process(
        target=_crash_checkpoint, args=(str(tmp_path), boundary))
    process.start(); process.join(10)
    assert process.exitcode == 92
    assert store.path.read_bytes() in {old, new}
    assert store.read()["value"] in {"old", "new"}
    assert all(path == store.path for path in tmp_path.glob("checkpoint.json"))

