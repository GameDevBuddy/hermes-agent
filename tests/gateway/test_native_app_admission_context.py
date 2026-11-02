"""The App binds its original context before a real SessionDB turn is admitted."""
import pytest

from gateway.native_app_context import require_context
from gateway.native_app_worker import require_completed_slot
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.native_app_producer_fixture import ProducerCase


def test_original_context_before_admission_completes_actual_claim_and_private_call(monkeypatch, tmp_path):
    case = ProducerCase(monkeypatch, tmp_path, claim_after_context=True)
    try:
        case.begin()
        assert case.call.context.stage.generation == 0 and case.claim["generation"] == 1

        async def unrelated_loop_consumer():
            return require_context(case.connection, case.ref)

        with pytest.raises(RuntimeStoreError):
            case.run(unrelated_loop_consumer())
        assert case.call.authenticated and case.call.origin is None
        case.phase("prepare_native_call", 5)
        prepared = case.run(case.notification("native_call_prepared"))
        assert prepared["proof"]["canonical"]["scope"]["generation"] == case.claim["generation"]
        case.deliver_result()
        committed = case.run(case.notification("native_call_committed"))
        assert committed["proof"]["canonical"]["row"]["generation"] == case.claim["generation"]
        for index, (command, event) in enumerate((
            ("ack_native_call", "native_call_acknowledged"),
            ("complete_native_call", "native_call_completed"),
            ("release_native_call", "native_call_released")), 6):
            case.phase(command, index)
            case.run(case.notification(event))
        case.run(case.worker_done.wait())
        assert case.worker_error is None and case.control_error is None
        assert require_completed_slot(case.slot) is case.call
    finally:
        case.close()
    assert case.keeper.completed
