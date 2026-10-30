"""Session construction, aliases and backend consumers tied to original browser owners."""
from __future__ import annotations

import functools

from tools import browser_owned as owned
from tools.browser_owned_launcher import require_browser


def current():
    record = owned._building.get()
    while record is not None:
        if record.kind == "session":
            return record
        record = record.original_parent
    command = owned._command.get()
    return getattr(command, "session", None)


def require_session(record):
    record.require()
    if record.obj is not None:
        info = record.obj
        state = (info.get("cdp_url"), info.get("launcher_key"), info.get("home_key"), info.get("session_key"))
        if state != record.state or info.get("features") != record.features or info.get("shared_profile"):
            owned.refuse(record, "original-browser-session-fields-changed")
    if record.browser is not None:
        require_browser(record.browser)
        if record.obj is not None and record.obj.get("cdp_url") != record.browser.obj.cdp_url:
            owned.refuse(record, "original-browser-session-launch-changed")


def session(factory):
    @functools.wraps(factory)
    def invoke(cache_key=None):
        from tools import browser_session as sessions
        key = cache_key or "default"
        with sessions._cleanup_lock:
            existing = sessions._active_sessions.get(key)
        if existing is not None:
            previous = owned.protect(existing)
            if previous is not None:
                require_session(previous)
                if previous.closed:
                    owned.refuse(previous, "original-browser-session-already-closed")
                attach_command(previous)
                return existing
        record = owned.reserve("session", factory)
        if record is None:
            return factory(cache_key)
        record.key, record.browser, record.supervisor = key, None, None
        record.state, record.features = None, None
        result = owned.call(record, factory, cache_key)
        if record.obj is not result:
            record.references += (result,)
            owned.refuse(record, "original-browser-published-session-was-replaced")
        require_session(record)
        token = owned._building.set(record)
        try:
            from tools.browser_session_cdp import ensure_cdp_supervisor
            ensure_cdp_supervisor(key)
            require_session(record)
        except BaseException as error:
            owned.fail(record, error)
            raise
        finally:
            owned._building.reset(token)
        attach_command(record)
        return result
    return invoke


def attach_command(record):
    command = owned._command.get()
    if command is not None:
        command.require()
        previous = getattr(command, "session", None)
        if previous is not None and previous is not record:
            owned.refuse(command, "original-browser-command-session-changed")
        command.session = record
        command.references += (record, record.obj)


def published(info):
    record = current()
    if record is None:
        if owned.active() is not None:
            owned.unsupported("session-published-without-constructor", info)
        return
    if record.obj is not None:
        record.references += (info,)
        owned.refuse(record, "original-browser-session-published-twice")
    owned.bind(record, info)
    record.state = (info.get("cdp_url"), info.get("launcher_key"), info.get("home_key"), info.get("session_key"))
    record.features = dict(info.get("features") or {})
    from tools import browser_launcher as launcher
    with launcher._lock:
        browser = launcher._browsers.get(info.get("launcher_key"))
    record.browser = owned.require_object(browser, "browser")
    require_session(record)


def backend_guard(force_local):
    if owned.active() is None:
        return
    from tools.browser_session_cdp import get_cdp_override_raw
    from tools.browser_session_cloud import browser_cfg, use_real_profile, get_browser_engine, user_data_dir
    if get_cdp_override_raw() and not force_local:
        owned.unsupported("external-cdp")
    provider = browser_cfg("cloud_provider", "auto")
    if not force_local and provider not in ("local", "camofox"):
        # Auto detection may instantiate a plugin SDK. Its foreign lifecycle is not adopted.
        owned.unsupported("cloud-provider-" + str(provider))
    if use_real_profile() or user_data_dir():
        owned.unsupported("shared-or-real-profile")
    if get_browser_engine() == "lightpanda":
        owned.unsupported("lightpanda-process")


def endpoint_guard(url):
    if owned.active() is None or not url:
        return
    record = current()
    if record is None or record.obj is None or record.browser is None:
        owned.unsupported("opaque-cdp-endpoint", url)
    require_session(record)
    if url != record.obj.get("cdp_url") or url != record.browser.obj.cdp_url:
        owned.refuse(record, "original-browser-endpoint-replaced")


def creator_guard(kind):
    if owned.active() is not None:
        owned.unsupported(kind)
