"""Original registration, submission, Future publication and worker/business release consumers."""
from __future__ import annotations

import functools
import inspect

from cron import scheduler_owned as owned
from cron.scheduler_owned_pools import require_pool

_entries, _registrations, _prepared, _callbacks, _futures = {}, {}, {}, {}, {}


def require_work(record, *, pending=True):
    record.require()
    record.parent.context.copy().run(lambda: require_pool(record.parent, pending=pending))
    job, process, pool = _entries[record]
    if (record.original_job is not job or record.process_job is not process or record.parent.obj is not pool
            or record.original_job.get("id") != record.job_id):
        owned.refuse(record, "cron-original-job-identity-changed")
    if record in _registrations and record.registration_owner is not _registrations[record]:
        owned.refuse(record, "cron-original-registration-owner-changed")
    if record in _prepared:
        execution, job, ctx, home = _prepared[record]
        if (record.execution is not execution or record.dispatched_job is not job or record.dispatch_context is not ctx
                or record.claim_home is not home or job.get("id") != record.job_id):
            owned.refuse(record, "cron-original-dispatch-context-or-job-changed")
    if record in _callbacks and (record.normal_worker, record.worker_callback) != _callbacks[record]:
        owned.refuse(record, "cron-original-worker-callable-changed")
    if record in _futures:
        future = _futures[record]
        if record.future is not future or future.parent is not record or owned.find(future.obj) is not future:
            owned.refuse(record, "cron-original-future-or-parent-changed")
        owned.require_methods(future)


def entry(factory):
    @functools.wraps(factory)
    def invoke(job, pool, process_job):
        parent = owned.protect(pool, "pool")
        if parent is None:
            return factory(job, pool, process_job)
        require_pool(parent)
        record = owned.reserve("work", factory, parent)
        token = None
        try:
            initialize(record, job, pool, process_job)
            token = owned._submission.set(record)
            record.result = factory(job, pool, process_job)
            require_work(record)
            if record.factory_attempted:
                if not record.submit_returned or record.result is not record.future.obj:
                    owned.refuse(record, "cron-original-submit-result-or-publication-changed")
                require_registration(record)
            elif record.registered:
                owned.refuse(record, "cron-registered-work-without-original-submission")
            else:
                record.done, record.active = True, False
            record.dispatch_returned = True
            return record.result
        except BaseException as error:
            owned.fail(record, error)
            raise
        finally:
            if token is not None:
                owned._submission.reset(token)
    return invoke


def initialize(record, job, pool, process_job):
    record.original_job, record.job_id, record.process_job = job, job["id"], process_job
    _entries[record] = (job, process_job, pool)
    record.registration_owner = record.execution = record.dispatched_job = record.dispatch_context = record.claim_home = None
    record.future = record.work_item = record.worker_thread = None
    record.registered = record.released = record.factory_attempted = record.submit_returned = False
    record.worker_entered = record.worker_finished = record.queue_attempted = record.queued = False
    record.dispatch_returned = False
    record.release_attempted = record.wait_attempted = record.wait_done = False
    record.arguments, record.references = (job, pool, process_job), (job, pool, process_job)
    record.attempted = record.active = True
    record.parent.works += (record,)


def registration(owner, admitted=None):
    record = owned._submission.get()
    if record is None:
        return
    require_work(record)
    if admitted is None:
        if record in _registrations:
            owned.refuse(record, "cron-original-registration-attempt-retained")
        record.registration_owner = owner
        _registrations[record] = owner
    else:
        if owner is not record.registration_owner:
            owned.refuse(record, "cron-original-registration-result-changed")
        record.registered = admitted


def prepared(execution, job, ctx, home):
    record = owned._submission.get()
    if record is not None:
        require_work(record)
        record.execution, record.dispatched_job, record.dispatch_context, record.claim_home = execution, job, ctx, home
        _prepared[record] = (execution, job, ctx, home)
        record.references += (execution, job, ctx, home)


def submission_failed(error):
    record = owned._submission.get()
    if record is not None:
        owned.fail(record, error)
        raise error


def worker(callback):
    record = owned._submission.get()
    if record is None:
        return callback
    require_work(record)
    record.normal_worker = callback

    def run():
        def original():
            from cron.scheduler_owned_executor import _current_thread
            require_work(record)
            if record.worker_entered:
                owned.refuse(record, "cron-original-worker-reentry")
            thread = owned.protect(_current_thread(), "thread")
            if thread is None or thread.parent is not record.parent:
                owned.refuse(record, "cron-worker-without-original-executor-thread")
            record.worker_thread, record.worker_entered = thread, True
            try:
                result = record.normal_worker()
                record.worker_result = result
                require_work(record)
                if inspect.isawaitable(result):
                    owned.refuse(record, "cron-original-synchronous-worker-returned-async-work")
                if not record.released:
                    owned.refuse(record, "cron-original-worker-business-release-unconfirmed")
                record.worker_finished = True
                return result
            except BaseException as error:
                owned.fail(record, error)
                raise
        return record.dispatch_context.copy().run(original)

    record.worker_callback = run
    _callbacks[record] = (callback, run)
    record.references += (callback, run)
    return run


def attach_future(record, future):
    if record in _futures:
        owned.refuse(record, "cron-original-future-constructor-attempt-retained")
    record.future = future
    _futures[record] = future


def observe_future(record):
    future = record.future
    def observed(actual):
        def original():
            try:
                require_work(record)
                if actual is not future.obj:
                    owned.refuse(record, "cron-original-completed-future-changed")
                if actual.cancelled():
                    actual.result()  # retain the actual normal API's first cancellation error
                error = actual.exception()
                if error is not None:
                    raise error
                future.done, future.active = True, False
            except BaseException as error:  # health: allow BLE001 -- keep the original completed Future and exact first error without retry/cancellation
                owned.fail(record, error)
        record.context.copy().run(original)
    record.future_observer = observed
    future.references += (observed, record)
    future.obj.add_done_callback(observed)


def require_registration(record):
    from cron import scheduler
    key = scheduler._inflight_key(record.job_id, record.claim_home)
    with scheduler._running_lock:
        current = scheduler._running_registration_owners.get(key)
        if not record.released and (current is not record.registration_owner
                                    or scheduler._running_futures.get(key) is not record.future.obj):
            owned.refuse(record, "cron-original-business-future-publication-changed")


def releasing(job_id, home, owner):
    matches = [record for record, value in _registrations.items() if value is owner]
    if not matches:
        return None
    record = matches[0]
    require_work(record)
    from cron import scheduler
    if scheduler._inflight_key(job_id, home) != scheduler._inflight_key(record.job_id, record.claim_home):
        owned.refuse(record, "cron-original-release-profile-or-job-changed")
    if not record.released and scheduler._running_registration_owners.get(
            scheduler._inflight_key(job_id, home)) is not owner:
        owned.refuse(record, "cron-original-business-owner-lost-before-release")
    record.release_attempted = True
    return record


def released(record):
    if record is not None:
        require_work(record)
        from cron import scheduler
        key = scheduler._inflight_key(record.job_id, record.claim_home)
        if scheduler._running_registration_owners.get(key) is record.registration_owner:
            owned.refuse(record, "cron-original-business-registration-not-released")
        record.released = True
