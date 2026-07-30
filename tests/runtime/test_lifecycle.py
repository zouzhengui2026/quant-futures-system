import json
from pathlib import Path

import pytest

from quant_futures.paper_runtime.control import audit, project_status, write_status
from quant_futures.paper_runtime.lifecycle import (
    LEGAL_TRANSITIONS,
    Lifecycle,
    LifecycleError,
    LifecycleState,
)


PATHS = {
    LifecycleState.CREATED: (),
    LifecycleState.STARTING: (LifecycleState.STARTING,),
    LifecycleState.RUNNING: (LifecycleState.STARTING, LifecycleState.RUNNING),
    LifecycleState.PAUSED: (LifecycleState.STARTING, LifecycleState.RUNNING, LifecycleState.PAUSED),
    LifecycleState.STOPPING: (LifecycleState.STARTING, LifecycleState.RUNNING, LifecycleState.STOPPING),
    LifecycleState.COMPLETED: (LifecycleState.STARTING, LifecycleState.RUNNING,
                               LifecycleState.STOPPING, LifecycleState.COMPLETED),
    LifecycleState.FAILED_RECOVERABLE: (LifecycleState.STARTING, LifecycleState.FAILED_RECOVERABLE),
    LifecycleState.RECOVERING: (LifecycleState.STARTING, LifecycleState.FAILED_RECOVERABLE,
                                LifecycleState.RECOVERING),
    LifecycleState.FAILED_TERMINAL: (LifecycleState.STARTING, LifecycleState.FAILED_RECOVERABLE,
                                     LifecycleState.FAILED_TERMINAL),
}


def lifecycle_at(tmp_path: Path, state: LifecycleState) -> Lifecycle:
    directory = tmp_path / state.value
    directory.mkdir()
    lifecycle = Lifecycle(directory)
    lifecycle.initialize("run-1")
    for target in PATHS[state]:
        lifecycle.transition(target, "test route")
    return lifecycle


@pytest.mark.parametrize(("source", "target"), sorted(LEGAL_TRANSITIONS, key=lambda edge: tuple(map(str, edge))))
def test_every_legal_transition_is_persisted(tmp_path: Path, source: LifecycleState,
                                              target: LifecycleState) -> None:
    lifecycle = lifecycle_at(tmp_path, source)
    record = lifecycle.transition(target, "operator request")
    assert record.state is target
    assert Lifecycle(lifecycle.run_directory).current() == record


@pytest.mark.parametrize(
    ("source", "target"),
    [(source, target) for source in LifecycleState for target in LifecycleState
     if (source, target) not in LEGAL_TRANSITIONS],
)
def test_every_illegal_transition_fails_closed_without_append(
    tmp_path: Path, source: LifecycleState, target: LifecycleState,
) -> None:
    lifecycle = lifecycle_at(tmp_path, source)
    before = lifecycle.path.read_bytes()
    with pytest.raises(LifecycleError, match="illegal lifecycle transition"):
        lifecycle.transition(target, "operator request")
    assert lifecycle.path.read_bytes() == before


def test_corrupt_authority_fails_closed_and_status_is_only_a_projection(tmp_path: Path) -> None:
    lifecycle = lifecycle_at(tmp_path, LifecycleState.RUNNING)
    status = write_status(lifecycle.run_directory)
    assert status["authoritative"] is False and status["authority"] == "lifecycle.jsonl"
    (lifecycle.run_directory / "status.json").write_text('{"lifecycle":"PAUSED"}\n')
    assert lifecycle.current().state is LifecycleState.RUNNING
    assert project_status(lifecycle.run_directory)["lifecycle"] == "RUNNING"
    assert not audit(lifecycle.run_directory)
    with lifecycle.path.open("a") as stream:
        stream.write("not-json\n")
    with pytest.raises(LifecycleError, match="invalid lifecycle record"):
        lifecycle.current()
