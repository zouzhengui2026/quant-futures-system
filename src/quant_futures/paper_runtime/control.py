"""Filesystem-facing lifecycle controls and non-authoritative status projection."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from pathlib import Path

from .lifecycle import Lifecycle, LifecycleError, LifecycleRecord, LifecycleState
from .journal import JournalError, JournalSnapshot, TransitionJournal
from .lock import RunDirectoryLock
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
    try:
        checkpoint = CheckpointStore(lifecycle.run_directory).read()
    except CheckpointError:
        checkpoint = None
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


def start(root: str | Path) -> Path:
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex
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


def recover(run_directory: str | Path) -> LifecycleRecord:
    """Repair product authority, then leave the run in its pre-crash disposition.

    Validation ambiguity is terminal.  A failure after an otherwise valid
    recovery has started is recoverable, so a later invocation may retry it.
    One invocation is represented by a ``started``/outcome pair.
    """
    directory = Path(run_directory)
    lifecycle = Lifecycle(run_directory)
    with RunDirectoryLock(run_directory):
        current = lifecycle.current()
        from .operations import RecoveryAttempts
        attempts = RecoveryAttempts(run_directory)
        metadata = _runtime_metadata(directory) if (directory / "runtime.json").exists() else None
        attempts.append_held(current.run_id, "started")
        journal = TransitionJournal(run_directory)
        try:
            journal_snapshot = journal._repair_tail_held()
        except BaseException:
            attempts.append_held(current.run_id, "failed")
            raise
        journal_tail = journal_snapshot.tail
        if journal_tail is not None and journal_tail.run_id != current.run_id:
            raise JournalError("journal run ID does not match lifecycle authority")
        checkpoint = CheckpointStore(run_directory).read() if CheckpointStore(run_directory).path.exists() else None
        checkpoint_digest = (checkpoint.get("journal", {}).get("digest")
                             if isinstance(checkpoint, dict) else None)
        incoherent = (metadata is not None and journal_tail is not None
                      and journal_tail.digest != checkpoint_digest)
        desired = (LifecycleState.PAUSED if current.state is LifecycleState.PAUSED
                   else LifecycleState.RUNNING)
        if current.state is LifecycleState.FAILED_RECOVERABLE:
            # The last healthy disposition is recovered from the legal lineage.
            prior = lifecycle.records()[-2].state if len(lifecycle.records()) > 1 else None
            desired = LifecycleState.PAUSED if prior is LifecycleState.PAUSED else LifecycleState.RUNNING
        if incoherent and current.state in {LifecycleState.RUNNING, LifecycleState.PAUSED}:
            lifecycle._transition_held(LifecycleState.FAILED_RECOVERABLE,
                                       "incomplete durable product transition detected")
            current = lifecycle.current()
        if incoherent and current.state is LifecycleState.FAILED_RECOVERABLE:
            lifecycle._transition_held(LifecycleState.RECOVERING, "recovery requested")
            current = lifecycle.current()
        if incoherent and current.state is not LifecycleState.RECOVERING:
            attempts.append_held(current.run_id, "failed")
            raise LifecycleError(f"lifecycle state is not recoverable: {current.state.value}")
        if incoherent and metadata is not None:
            try:
                _recover_product_suffix_held(Path(run_directory), current.run_id,
                                             journal, journal_snapshot, checkpoint)
                journal_snapshot = journal._snapshot_held()
                lifecycle._transition_held(desired, "product authority recovered")
                attempts.append_held(current.run_id, "recovered")
                _write_status_held(lifecycle, journal_snapshot)
                return lifecycle.current()
            except BaseException as exc:
                attempts.append_held(current.run_id, "failed")
                if lifecycle.current().state is LifecycleState.RECOVERING:
                    # Deterministic rule: unverifiable durable input is terminal;
                    # environmental/runtime failures remain retryable.
                    target = (LifecycleState.FAILED_TERMINAL
                              if isinstance(exc, (JournalError, CheckpointError, ValueError))
                              else LifecycleState.FAILED_RECOVERABLE)
                    lifecycle._transition_held(target, "recovery attempt failed")
                raise
        if incoherent:
            attempts.append_held(current.run_id, "failed")
            if lifecycle.current().state is LifecycleState.RECOVERING:
                lifecycle._transition_held(LifecycleState.FAILED_TERMINAL,
                                           "runtime metadata required for product recovery")
            raise LifecycleError("runtime metadata required for product recovery")
        if current.state in {LifecycleState.RUNNING, LifecycleState.PAUSED}:
            attempts.append_held(current.run_id, "no-op")
            _write_status_held(lifecycle, journal_snapshot)
            return current
        if current.state is not LifecycleState.FAILED_RECOVERABLE:
            attempts.append_held(current.run_id, "failed")
            raise LifecycleError(f"lifecycle state is not recoverable: {current.state.value}")
        lifecycle._transition_held(LifecycleState.RECOVERING, "recovery requested")
        record = lifecycle._transition_held(desired, "lifecycle authority validated")
        attempts.append_held(current.run_id, "recovered")
        _write_status_held(lifecycle, journal_snapshot)
        return record


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


def continue_runtime(run_directory: str | Path, *, install_signals: bool = False) -> object:
    """Reopen one existing run and consume only bars after its durable cursor."""
    from dataclasses import replace
    from quant_futures.product.config import load_config
    from quant_futures.product.data import load_bars
    from quant_futures.product.strategy import build_strategy
    from .operations import PaperRuntime, StopFlag
    from .transition import PaperTransitionCoordinator

    directory = Path(run_directory)
    metadata = _runtime_metadata(directory)
    lifecycle = Lifecycle(directory).current()
    if lifecycle.state is LifecycleState.PAUSED:
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
                        stop_flag=flag).run(bars[cursor:])


def audit(run_directory: str | Path) -> bool:
    """Validate lifecycle authority and require the projection to match it exactly."""
    from .operations import OperationalError
    try:
        with RunDirectoryLock(run_directory):
            expected = _project_status_held(Lifecycle(run_directory))
            actual = json.loads((Path(run_directory) / "status.json").read_text(encoding="utf-8"))
    except (LifecycleError, JournalError, OSError, UnicodeDecodeError,
            json.JSONDecodeError, ValueError, OperationalError):
        return False
    return actual == expected
