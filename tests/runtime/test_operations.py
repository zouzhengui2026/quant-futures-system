import signal

from quant_futures.paper_runtime import (OperationalRequests, PaperRuntime,
                                         RecoveryAttempts, StopFlag)
from quant_futures.paper_runtime.journal import TransitionJournal
from quant_futures.product.strategy import FixedStrategy

from test_transition import authorized_coordinator, bar, config


def test_finite_runtime_stops_only_after_committed_boundary(tmp_path):
    coordinator = authorized_coordinator(
        "runtime", config(), FixedStrategy(1.0), TransitionJournal(tmp_path))
    flag = StopFlag()
    boundaries = []

    def inject(boundary):
        if boundary == "journal:strategy_committed":
            flag.handler(signal.SIGTERM, None)
            boundaries.append(coordinator.state.input_cursor)

    coordinator._failure_injector = inject
    result = PaperRuntime(coordinator, stop_flag=flag).run((bar(0), bar(1)))
    assert boundaries == [0]
    assert result.processed == 1
    assert result.state.input_cursor == 1
    assert result.stopped


def test_operational_requests_are_monotonic_and_idempotent_at_boundary(tmp_path):
    coordinator = authorized_coordinator(
        "requests", config(), FixedStrategy(0.0), TransitionJournal(tmp_path))
    requests = OperationalRequests(tmp_path)
    assert requests.request("resume")["sequence"] == 1
    assert requests.request("resume")["sequence"] == 2
    result = PaperRuntime(coordinator).run((bar(0),))
    assert result.processed == 1
    assert result.state.input_cursor == 1


def test_recovery_attempt_authority_is_monotonic_and_hash_chained(tmp_path):
    attempts = RecoveryAttempts(tmp_path)
    assert attempts.append_held("run", "started") == 1
    assert attempts.append_held("run", "recovered") == 2
    records = attempts.read()
    assert [record["sequence"] for record in records] == [1, 2]
    assert records[1]["previous_digest"] == records[0]["digest"]
