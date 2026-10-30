"""Actual before-allocation claims and partial submission objects; synthetic failures only."""
import concurrent.futures

import pytest

from tests.cron.owned_fixture import submit

pytest_plugins = ("tests.cron.owned_fixture",)
pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize("stage", ["future-new", "future-init", "work-item"])
async def test_original_future_and_work_item_failure_retains_claims_before_factories(cron_fixture, monkeypatch, stage):
    import concurrent.futures.thread as backend
    from hermes_owned_gateway import OwnedGatewayUnknown
    f = cron_fixture
    f.activate()
    failure = MemoryError("synthetic allocation failure, not real exhaustion")
    seen = []
    future_class = concurrent.futures.Future

    class Future(future_class):
        def __new__(cls):
            if stage == "future-new":
                assert any(r.kind == "future" and r.attempted for r in f.source.cron_records)
                raise failure
            return object.__new__(cls)

        def __init__(self):
            record = f.owned.find(self)
            assert record is not None and record.obj is self and record.attempted
            seen.append(record)
            if stage == "future-init":
                raise failure
            super().__init__()

    class WorkItem:
        def __init__(self, *args):
            record = f.owned.find(self)
            assert record is not None and record.attempted
            seen.append(record)
            raise failure

    monkeypatch.setattr(concurrent.futures, "Future", Future)
    if stage == "work-item":
        monkeypatch.setattr(backend, "_WorkItem", WorkItem)
    with pytest.raises(MemoryError) as caught:
        submit(f)
    assert caught.value is failure and f.source.cron_error is failure and f.source.cron_credit > 0
    assert not f.threads and not f.pools[0]._work_queue.items
    assert f.scheduler._running_futures and all(r in f.source.cron_records for r in seen)
    count = len(f.source.cron_records)
    with pytest.raises(OwnedGatewayUnknown):
        submit(f)
    assert len(f.source.cron_records) == count and f.source.cron_error is failure
