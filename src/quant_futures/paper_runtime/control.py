"""Filesystem-facing lifecycle controls and non-authoritative status projection."""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Callable

from .lifecycle import Lifecycle, LifecycleError, LifecycleRecord, LifecycleState
from .journal import JournalError, JournalSnapshot, TransitionJournal
from .lock import RunDirectoryLock
from .lock import RuntimeConsumerLease
from .checkpoint import CheckpointError, CheckpointStore


def _atomic_projection(path: Path, value: dict[str, object]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def project_status(run_directory: str | Path) -> dict[str, object]:
    """Rebuild a lock-consistent snapshot from both persisted authorities."""
    directory = Path(run_directory)
    with RunDirectoryLock(directory):
        return _project_status_held(Lifecycle(directory))


def _project_status_held(
    lifecycle: Lifecycle, journal_snapshot: JournalSnapshot | None = None,
) -> dict[str, object]:
    record = lifecycle.current()
    if journal_snapshot is None:
        journal_snapshot = TransitionJournal(lifecycle.run_directory)._snapshot_held()
    journal_tail = journal_snapshot.tail
    if journal_tail is not None and journal_tail.run_id != record.run_id:
        raise JournalError("journal run ID does not match lifecycle authority")
    status = _status_for_record(record, journal_tail)
    # A checkpoint is authoritative only when it names the validated committed
    # tail.  Status never attempts to repair or infer trading state.
    checkpoint_path = lifecycle.run_directory / CheckpointStore.filename
    checkpoint = CheckpointStore(lifecycle.run_directory).read() if checkpoint_path.exists() else None
    if (checkpoint is not None and journal_tail is not None
            and isinstance(checkpoint.get("journal"), dict)
            and checkpoint["journal"].get("digest") == journal_tail.digest):
        execution = checkpoint.get("execution", {})
        status.update({
            "pending_order": execution.get("pending_order_id"),
            "positions": checkpoint.get("portfolio", []),
            "equity": checkpoint.get("account", {}).get("equity"),
            "risk_outcome": checkpoint.get("risk", {}).get("outcome"),
            "counters": checkpoint.get("counters", status["counters"]),
            "last_committed_ordering_key": checkpoint.get("last_committed_ordering_key"),
        })
    from .operations import RecoveryAttempts
    attempts = RecoveryAttempts(lifecycle.run_directory)
    attempts.validate_checkpoint_anchor()
    status["recovery_attempts"] = attempts.attempt_count()
    coherent = (journal_tail is None and checkpoint is None) or (
        checkpoint is not None and journal_tail is not None
        and checkpoint.get("journal", {}).get("digest") == journal_tail.digest)
    suffix_kind = ("coherent" if coherent else
                   _classify_suffix(lifecycle.run_directory, checkpoint, journal_snapshot)
                   if (lifecycle.run_directory / "runtime.json").exists() else "terminal")
    status["last_durable_stage"] = getattr(journal_tail, "stage", None)
    status["recoverable"] = suffix_kind == "recoverable"
    status["terminal"] = (record.state is LifecycleState.FAILED_TERMINAL
                          or suffix_kind == "terminal")
    status["health"] = "healthy" if coherent and record.state in {
        LifecycleState.RUNNING, LifecycleState.PAUSED, LifecycleState.COMPLETED
    } else "attention"
    status["stalled"] = suffix_kind != "coherent"
    owner_live = RuntimeConsumerLease.is_owned(lifecycle.run_directory)
    status["consumer_owner"] = "live" if owner_live else "relinquished"
    if record.state is LifecycleState.RUNNING and not owner_live:
        status["health"] = "stalled"
        status["stalled"] = True
    return status


def _classify_suffix(directory: Path, checkpoint: dict[str, object] | None,
                     snapshot: JournalSnapshot) -> str:
    """Classify only a structurally legal sole product-transition suffix."""
    try:
        from .transition import StageProtocol
        records = tuple(TransitionJournal(directory).iter_records())
        base = int(checkpoint["journal"]["sequence"]) if checkpoint else 0
        suffix = records[base:]
        if not suffix or len({r.run_id for r in suffix}) != 1:
            return "terminal"
        if len({r.product_transition_id for r in suffix}) != 1:
            return "terminal"
        if len({r.effective_market_timestamp for r in suffix}) != 1:
            return "terminal"
        input_ids = {r.payload.get("input_event_id") for r in suffix}
        if len(input_ids) != 1:
            return "terminal"
        protocol = StageProtocol()
        for record in suffix:
            protocol.accept(record.stage)
        return "recoverable"
    except Exception:
        return "terminal"


def write_status(run_directory: str | Path) -> dict[str, object]:
    with RunDirectoryLock(run_directory):
        return _write_status_held(Lifecycle(run_directory))


def _write_status_held(
    lifecycle: Lifecycle, journal_snapshot: JournalSnapshot | None = None,
) -> dict[str, object]:
    status = _project_status_held(lifecycle, journal_snapshot)
    _atomic_projection(lifecycle.run_directory / "status.json", status)
    return status


def _status_for_record(record: LifecycleRecord, journal_tail: object | None = None) -> dict[str, object]:
    return {"schema_version": 1, "authoritative": False, "authority": Lifecycle.filename,
            "run_id": record.run_id, "lifecycle": record.state.value,
            "lifecycle_sequence": record.sequence, "last_reason": record.reason,
            "input_cursor": getattr(journal_tail, "input_cursor", 0),
            "journal_sequence": getattr(journal_tail, "sequence", 0),
            "journal_tail_digest": getattr(journal_tail, "digest", None),
            "pending_order": None,
            "positions": [], "equity": None, "risk_outcome": None,
            "counters": {"inputs": 0, "orders": 0, "fills": 0}}


def start(root: str | Path, *, _test_run_id: str | None = None) -> Path:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex if _test_run_id is None else _test_run_id
    if _test_run_id is not None and (
        len(run_id) != 32 or any(character not in "0123456789abcdef" for character in run_id)
    ):
        raise LifecycleError("test run ID must be 32 lowercase hexadecimal characters")
    directory = root / run_id
    directory.mkdir(mode=0o700)
    lifecycle = Lifecycle(directory)
    with RunDirectoryLock(directory):
        lifecycle._initialize_held(run_id)
        lifecycle._transition_held(LifecycleState.STARTING, "start requested")
        lifecycle._transition_held(LifecycleState.RUNNING, "checkpoint-one control plane initialized")
        _write_status_held(lifecycle)
    return directory


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_runtime_metadata(run_directory: str | Path, config: Path, replay: Path,
                           pace_seconds: float = 0.0) -> None:
    """Persist immutable reopen identities before the runtime consumes input."""
    directory = Path(run_directory)
    with RunDirectoryLock(directory):
        path = directory / "runtime.json"
        if path.exists():
            raise LifecycleError("runtime metadata already exists")
        config, replay = config.resolve(), replay.resolve()
        value = {"schema_version": 2, "config": str(config), "replay": str(replay),
                 "config_sha256": _file_sha256(config),
                 "replay_sha256": _file_sha256(replay),
                 "pace_seconds": float(pace_seconds)}
        _atomic_projection(path, value)
        from .lifecycle import _fsync_directory
        _fsync_directory(directory)


def _runtime_metadata(directory: Path) -> dict[str, object]:
    try:
        value = json.loads((directory / "runtime.json").read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise LifecycleError(f"invalid runtime metadata: {exc}") from exc
    fields = {"schema_version", "config", "replay", "config_sha256",
              "replay_sha256", "pace_seconds"}
    if (not isinstance(value, dict) or set(value) != fields or value["schema_version"] != 2
            or not isinstance(value["config"], str) or not isinstance(value["replay"], str)
            or not isinstance(value["config_sha256"], str)
            or not isinstance(value["replay_sha256"], str)
            or type(value["pace_seconds"]) not in {int, float}
            or value["pace_seconds"] < 0):
        raise LifecycleError("invalid runtime metadata schema")
    config, replay = Path(value["config"]), Path(value["replay"])
    if (_file_sha256(config) != value["config_sha256"]
            or _file_sha256(replay) != value["replay_sha256"]):
        raise LifecycleError("runtime config or replay identity changed")
    return value


def transition(run_directory: str | Path, target: LifecycleState, reason: str) -> LifecycleRecord:
    lifecycle = Lifecycle(run_directory)
    with RunDirectoryLock(run_directory):
        record = lifecycle._transition_held(target, reason)
        _write_status_held(lifecycle)
        return record


def stop(run_directory: str | Path) -> LifecycleRecord:
    lifecycle = Lifecycle(run_directory)
    with RunDirectoryLock(run_directory):
        lifecycle._transition_held(LifecycleState.STOPPING, "stop requested")
        record = lifecycle._transition_held(
            LifecycleState.COMPLETED, "checkpoint-one control plane stopped")
        _write_status_held(lifecycle)
        return record


def resume_runtime(run_directory: str | Path, *, install_signals: bool = False) -> object:
    """Atomically hand a relinquished PAUSED run to exactly one new consumer.

    The newer durable resume request and RUNNING lifecycle transition share the
    run lock.  The returned sequence is handed directly to the new runtime as
    its consumed baseline, so the pause request that stopped the old owner can
    never be replayed while any still-later request remains observable.
    """
    from .operations import OperationalRequests

    directory = Path(run_directory)
    with RunDirectoryLock(directory):
        lifecycle = Lifecycle(directory)
        state = lifecycle.current().state
        if state is not LifecycleState.PAUSED:
            raise LifecycleError("runtime resume requires PAUSED authority")
        request = OperationalRequests(directory)._request_held("resume")
        lifecycle._transition_held(LifecycleState.RUNNING, "existing runtime reopened")
        _write_status_held(lifecycle)
        baseline = int(request["sequence"])
    return continue_runtime(directory, install_signals=install_signals,
                            request_baseline=baseline)


def request_stop(run_directory: str | Path) -> LifecycleRecord | dict[str, object]:
    """Stop a relinquished PAUSED run directly, or request a live boundary stop."""
    from .operations import OperationalRequests

    directory = Path(run_directory)
    with RunDirectoryLock(directory):
        lifecycle = Lifecycle(directory)
        state = lifecycle.current().state
        if state is LifecycleState.RUNNING:
            return OperationalRequests(directory)._request_held("stop")
        if state is LifecycleState.PAUSED:
            OperationalRequests(directory)._request_held("stop")
            lifecycle._transition_held(LifecycleState.STOPPING,
                                       "relinquished paused runtime stopped")
            record = lifecycle._transition_held(
                LifecycleState.COMPLETED, "final committed boundary retained")
            _write_status_held(lifecycle)
            return record
        if state is LifecycleState.COMPLETED:
            return lifecycle.current()
        raise LifecycleError(f"runtime stop is illegal from {state.value}")


def _publish_recovery_commitment_held(
    directory: Path, failure_injector: Callable[[str], None] | None = None,
) -> None:
    """Atomically bind the completed recovery invocation into the checkpoint."""
    from .operations import RecoveryAttempts
    store = CheckpointStore(directory, failure_injector)
    if not store.path.exists():
        return
    document = store.read()
    records = RecoveryAttempts(directory).read()
    document["recovery_counter"] = sum(r["outcome"] == "started" for r in records)
    document["recovery_digest"] = records[-1]["digest"] if records else None
    store._write_held(document)


def _reconcile_recovery_publication_held(
    directory: Path, run_id: str,
    failure_injector: Callable[[str], None] | None = None,
) -> None:
    """Finish the previous recovery publication protocol before a new attempt.

    Recovery has two durable authorities and an atomic checkpoint commitment.
    Consequently a process may stop after either JSONL append, or after the
    checkpoint replacement whose directory fsync reported an indeterminate
    result.  Only the exact old/new commitment states below are admissible;
    every other lineage is ambiguous and fails closed.
    """
    from .operations import OperationalError, RecoveryAttempts

    attempts = RecoveryAttempts(directory)
    records = attempts.read()
    store = CheckpointStore(directory)
    if not store.path.exists():
        if records and records[-1]["outcome"] == "started":
            attempts.append_held(run_id, "failed")
        return
    checkpoint = store.read()
    count = checkpoint.get("recovery_counter")
    digest = checkpoint.get("recovery_digest")
    actual_count = sum(record["outcome"] == "started" for record in records)
    tail_digest = records[-1]["digest"] if records else None
    if (count, digest) == (actual_count, tail_digest):
        # A checkpoint naming an open start can only have been produced after
        # successful suffix reconstruction.  Complete that invocation once.
        if records and records[-1]["outcome"] == "started":
            attempts.append_held(run_id, "recovered")
            _publish_recovery_commitment_held(directory, failure_injector)
        return
    if not records:
        raise OperationalError("recovery checkpoint commitment is ambiguous")
    last = records[-1]
    if last["outcome"] == "started":
        previous_digest = records[-2]["digest"] if len(records) > 1 else None
        if (count, digest) != (actual_count - 1, previous_digest):
            raise OperationalError("recovery start commitment is ambiguous")
        attempts.append_held(run_id, "failed")
        _publish_recovery_commitment_held(directory, failure_injector)
        return
    # The outcome append is durable but the commitment is still allowed to
    # name its immediately preceding start.  Publish it; do not append again.
    previous = records[-2] if len(records) > 1 else None
    before_start_digest = records[-3]["digest"] if len(records) > 2 else None
    allowed = {(actual_count, previous["digest"] if previous else None),
               (actual_count - 1, before_start_digest)}
    if (previous is None or previous["outcome"] != "started"
            or (count, digest) not in allowed):
        raise OperationalError("recovery outcome commitment is ambiguous")
    _publish_recovery_commitment_held(directory, failure_injector)


def _desired_recovery_disposition(lifecycle: Lifecycle) -> LifecycleState:
    """Preserve the last operational disposition across arbitrarily many retries."""
    for record in reversed(lifecycle.records()):
        if record.state in {LifecycleState.RUNNING, LifecycleState.PAUSED}:
            return record.state
    return LifecycleState.RUNNING


def recover(
    run_directory: str | Path,
    *,
    failure_injector: Callable[[str], None] | None = None,
) -> LifecycleRecord:
    """Close exactly one durable recovery attempt and publish its commitment."""
    directory = Path(run_directory)
    # Recovery is itself a consumer.  Fail before lifecycle, journal,
    # checkpoint, Product authority, or recovery-attempt mutation when a live
    # runtime owns the replay.
    consumer_lease = RuntimeConsumerLease(directory).acquire()
    lifecycle = Lifecycle(directory)
    try:
        with RunDirectoryLock(directory):
          from .operations import RecoveryAttempts
          attempts = RecoveryAttempts(directory)
          current = lifecycle.current()
          if current.state is LifecycleState.COMPLETED:
              raise LifecycleError("completed runtime cannot be recovered")
          run_id = current.run_id
          desired = _desired_recovery_disposition(lifecycle)
          inject = failure_injector or (lambda _boundary: None)
          _reconcile_recovery_publication_held(directory, run_id, inject)
          attempts.validate_checkpoint_anchor()
          attempts.append_held(run_id, "started")
          inject("recovery_started_durable")
          closed = False

          def close(outcome: str) -> None:
              nonlocal closed
              if closed:
                  raise LifecycleError("recovery invocation already closed")
              attempts.append_held(run_id, outcome)
              inject("recovery_outcome_durable")
              closed = True
              _publish_recovery_commitment_held(directory, inject)
              inject("recovery_commitment_published")

          try:
              metadata = _runtime_metadata(directory) if (directory / "runtime.json").exists() else None
              journal = TransitionJournal(directory)
              journal_snapshot = journal._repair_tail_held()
              tail = journal_snapshot.tail
              if tail is not None and tail.run_id != run_id:
                  raise JournalError("journal run ID does not match lifecycle authority")
              store = CheckpointStore(directory)
              store._remove_orphaned_temporaries_held()
              checkpoint = store.read() if store.path.exists() else None
              checkpoint_digest = (checkpoint.get("journal", {}).get("digest")
                                   if isinstance(checkpoint, dict) else None)
              incoherent = bool(metadata is not None and tail is not None
                                and tail.digest != checkpoint_digest)
              state = lifecycle.current().state
              if incoherent and state in {LifecycleState.RUNNING, LifecycleState.PAUSED}:
                  lifecycle._transition_held(LifecycleState.FAILED_RECOVERABLE,
                                             "incomplete durable product transition detected")
                  state = LifecycleState.FAILED_RECOVERABLE
              if state is LifecycleState.FAILED_RECOVERABLE:
                  lifecycle._transition_held(LifecycleState.RECOVERING, "recovery requested")
                  state = LifecycleState.RECOVERING
              if incoherent:
                  if state is not LifecycleState.RECOVERING:
                      raise LifecycleError(f"lifecycle state is not recoverable: {state.value}")
                  if metadata is None:
                      raise LifecycleError("runtime metadata required for product recovery")
                  _recover_product_suffix_held(directory, run_id, journal, journal_snapshot, checkpoint)
                  inject("recovery_suffix_checkpoint_durable")
                  journal_snapshot = journal._snapshot_held()
                  lifecycle._transition_held(desired, "product authority recovered")
                  close("recovered")
                  _write_status_held(lifecycle, journal_snapshot)
                  return lifecycle.current()
              if state is LifecycleState.RECOVERING:
                  lifecycle._transition_held(desired, "lifecycle authority validated")
                  close("recovered")
              elif state in {LifecycleState.RUNNING, LifecycleState.PAUSED}:
                  close("no-op")
              else:
                  raise LifecycleError(f"lifecycle state is not recoverable: {state.value}")
              _write_status_held(lifecycle, journal_snapshot)
              return lifecycle.current()
          except BaseException as exc:
              if not closed:
                  attempts.append_held(run_id, "failed")
                  closed = True
                  # Keep retries legal and preserve the original desired disposition.
                  try:
                      if lifecycle.current().state is LifecycleState.RECOVERING:
                          lifecycle._transition_held(LifecycleState.FAILED_RECOVERABLE,
                                                     f"recovery attempt failed: {type(exc).__name__}")
                  finally:
                      # This may legitimately fail when the checkpoint itself is corrupt;
                      # the closed attempt chain still permits the next repair attempt.
                      try:
                          _publish_recovery_commitment_held(directory, inject)
                      except (CheckpointError, OSError, ValueError):
                          pass
              raise
    finally:
        consumer_lease.release()
        # The projection written inside the recovery transaction observed the
        # recovery lease itself. Republish after relinquishment so status does
        # not claim a live replay consumer. Authority was already committed.
        if sys.exc_info()[0] is None:
            write_status(directory)

def _recover_product_suffix_held(directory: Path, run_id: str,
                                 journal: TransitionJournal,
                                 snapshot: JournalSnapshot,
                                 checkpoint: dict[str, object] | None) -> None:
    """Validate and deterministically finish the sole journal suffix."""
    from quant_futures.product.config import load_config
    from quant_futures.product.data import Bar, load_bars
    from quant_futures.product.strategy import build_strategy
    from .transition import PaperTransitionCoordinator, TransitionError
    from dataclasses import replace

    try:
        metadata = _runtime_metadata(directory)
        config = load_config(str(metadata["config"]))
        config = replace(config, data=replace(config.data, path=str(Path(str(metadata["replay"])).resolve())))
        bars, fingerprint = load_bars(str(metadata["replay"]), config.data.schema,
                                      start=config.data.start, end=config.data.end,
                                      timeframe=config.data.timeframe)
        strategy = build_strategy(config.strategy.name, config.strategy.parameters)
        base_sequence = int(checkpoint["journal"]["sequence"]) if checkpoint else 0
        records = tuple(journal.iter_records())
        if base_sequence < 0 or base_sequence > len(records):
            raise TransitionError("checkpoint journal cursor is outside the journal")
        base = JournalSnapshot(records[base_sequence - 1] if base_sequence else None)
        suffix = records[base_sequence:]
        if not suffix or len({r.product_transition_id for r in suffix}) != 1:
            raise TransitionError("recovery requires exactly one incomplete transition suffix")
        cursor = (int(checkpoint["cursor"]) if checkpoint else 0) + 1
        if cursor > len(bars):
            raise TransitionError("recovery cursor exceeds replay input")
        bar: Bar = bars[cursor - 1]
        coordinator = PaperTransitionCoordinator(
            run_id, config, strategy, journal,
            data_fingerprint=f"sha256:{fingerprint}", _recovery_base=base,
            _recover_from_empty=checkpoint is None, _lock_held=True,
        )
        coordinator._transition_held(bar, suffix)
    except (OSError, KeyError, TypeError, ValueError) as exc:
        raise TransitionError(f"cannot recover product transition: {exc}") from exc


def continue_runtime(run_directory: str | Path, *, install_signals: bool = False,
                     request_baseline: int = 0) -> object:
    """Reopen one existing run and consume only bars after its durable cursor."""
    from dataclasses import replace
    from quant_futures.product.config import load_config
    from quant_futures.product.data import load_bars
    from quant_futures.product.strategy import build_strategy
    from .operations import PaperRuntime, StopFlag
    from .transition import PaperTransitionCoordinator

    directory = Path(run_directory)
    lease = RuntimeConsumerLease(directory).acquire()
    try:
        metadata = _runtime_metadata(directory)
        lifecycle = Lifecycle(directory).current()
        if lifecycle.state is LifecycleState.PAUSED:
            lease.release()
            return None
        if lifecycle.state is not LifecycleState.RUNNING:
            raise LifecycleError("runtime continuation requires RUNNING or PAUSED authority")
        config = load_config(str(metadata["config"]))
        replay = Path(str(metadata["replay"]))
        effective = replace(config, data=replace(config.data, path=str(replay)))
        bars, fingerprint = load_bars(replay, effective.data.schema, start=effective.data.start,
                                      end=effective.data.end, timeframe=effective.data.timeframe)
        coordinator = PaperTransitionCoordinator(
            lifecycle.run_id, effective,
            build_strategy(effective.strategy.name, effective.strategy.parameters),
            TransitionJournal(directory), data_fingerprint=f"sha256:{fingerprint}")
        cursor = coordinator.state.input_cursor
        if cursor > len(bars):
            raise LifecycleError("checkpoint cursor exceeds normalized replay")
        flag = StopFlag()
        if install_signals:
            flag.install()
        return PaperRuntime(coordinator, pace_seconds=float(metadata["pace_seconds"]),
                            stop_flag=flag, request_baseline=request_baseline,
                            consumer_lease=lease).run(bars[cursor:])
    except BaseException:
        lease.release()
        raise


def audit(run_directory: str | Path) -> bool:
    """Validate every runtime authority and its disposable projection.

    Audit is deliberately stricter than recovery: it never repairs an
    interrupted publication.  Exact protocol temporary files are evidence of
    an unresolved publication and therefore make the run fail closed.
    """
    from .operations import OperationalError
    try:
        with RunDirectoryLock(run_directory):
            directory = Path(run_directory)
            lifecycle = Lifecycle(directory)
            expected = _project_status_held(lifecycle)
            actual = json.loads((directory / "status.json").read_text(encoding="utf-8"))
            # Runtime metadata is recovery-critical authority.  Cross-check
            # both content hashes even for an otherwise terminal run.
            if (directory / "runtime.json").exists():
                _runtime_metadata(directory)
            # These exact names are created by our atomic publication
            # protocols.  Similarly named operator files are not rejected.
            temporary_patterns = (
                ".checkpoint.json.*.tmp",
                ".status.json.*.tmp",
                ".runtime.json.*.tmp",
                ".control-request.json.*.tmp",
            )
            if any(candidate.exists() or candidate.is_symlink() for pattern in temporary_patterns
                   for candidate in directory.glob(pattern)):
                return False
            # Once product authority exists, its complete-boundary checkpoint
            # is independently mandatory.  Disposable status must never make
            # a missing, malformed, stale, or foreign checkpoint acceptable.
            checkpoint_path = directory / CheckpointStore.filename
            journal = TransitionJournal(directory)._snapshot_held()
            product_protocol = (directory / "runtime.json").exists()
            if journal.tail is not None and product_protocol:
                checkpoint = CheckpointStore(directory).read()
                authority = checkpoint.get("journal")
                # Validate the protected Product-state commitment independently
                # of the disposable projection.  This catches type-valid edits
                # to nested checkpoint authority as well as malformed bytes.
                from .transition import _bounded_state_digest
                state_digest = checkpoint.get("state_digest")
                if (not isinstance(authority, dict)
                        or authority.get("sequence") != journal.tail.sequence
                        or authority.get("digest") != journal.tail.digest
                        or checkpoint.get("run_id") != lifecycle.current().run_id
                        or authority.get("input_cursor") != journal.tail.input_cursor
                        or not isinstance(state_digest, str)
                        or state_digest != _bounded_state_digest(checkpoint)
                        or journal.tail.payload.get("state_digest") != state_digest):
                    return False
            elif checkpoint_path.exists() and journal.tail is None:
                # A checkpoint without a committed product journal has no
                # coherent authority boundary.
                return False
    except (LifecycleError, JournalError, CheckpointError, OSError, UnicodeDecodeError,
            json.JSONDecodeError, ValueError, OperationalError):
        return False
    return actual == expected
