"""Only the original diagnostic child/readers may observe normal exit and EOF."""
from __future__ import annotations

import subprocess
import types

from hermes_cli._subprocess_compat import windows_hide_flags
from hermes_owned_gateway import OwnedGatewayUnknown

_popen_type = subprocess.Popen
OUTPUT_BYTES = 64 * 1024


def _same_method(stream, name, original, descriptor):
    from agent.auxiliary_owned_close import _same_callable
    actual = getattr(stream, name)
    return (_same_callable(actual, original)
            or (type(actual) is types.BuiltinMethodType and type(original) is types.BuiltinMethodType
                and actual.__self__ is original.__self__ is stream
                and actual.__name__ == original.__name__ == name
                and getattr(type(stream), name, None) is descriptor))


def _read(owner, record, channel):
    from tools.env_probe_owned import fail
    stream = record["pipes"][channel]
    read = record["read"][channel]
    try:
        while True:
            owner.require()
            if (record["pipes"][channel] is not stream
                    or not _same_method(stream, "read", read, record["read_types"][channel])):
                raise OwnedGatewayUnknown("original-probe-pipe-reader-changed")
            raw = read(8192)
            if (record["pipes"][channel] is not stream
                    or not _same_method(stream, "read", read, record["read_types"][channel])):
                raise OwnedGatewayUnknown("original-probe-pipe-reader-changed")
            if type(raw) is not bytes:
                raise OwnedGatewayUnknown("original-probe-pipe-did-not-return-bytes")
            if not raw:
                record["eof"][channel] = True
                return
            if len(record["output"][channel]) + len(raw) > OUTPUT_BYTES:
                raise OwnedGatewayUnknown("original-probe-output-credit-exhausted")
            record["output"][channel].extend(raw)
            if record["pipes"][channel] is not stream:
                raise OwnedGatewayUnknown("original-probe-pipe-displaced")
    except BaseException as error:
        fail(owner.source, error)
        raise


def run_command(owner, record):
    from agent.auxiliary_owned_close import _same_callable
    from tools.env_probe_owned import construct, fail, require_node, require_thread
    try:
        kwargs = dict(stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                      env=owner.environment, cwd=owner.cwd, close_fds=True,
                      creationflags=windows_hide_flags())
        proc = construct(owner, record["process"], _popen_type, (record["argv"],), kwargs)
        record["pipes"], record["wait"] = (proc.stdout, proc.stderr), proc.wait
        record["read"] = (proc.stdout.read, proc.stderr.read)
        record["read_types"] = tuple(getattr(type(stream), "read", None) for stream in record["pipes"])
        record["close"] = tuple(stream.close for stream in record["pipes"])
        record["close_types"] = tuple(getattr(type(stream), "close", None) for stream in record["pipes"])
        for channel, node in enumerate(record["readers"]):
            owner.start_thread(node, _read, name=f"env-probe-reader-{channel}", args=(owner, record, channel))
        # Popen.wait's timeout never kills. Original readers/child remain held on failure.
        record["exit"] = record["wait"](timeout=record["timeout"])
        for node in record["readers"]:
            require_thread(owner, node)
            node["methods"][1]()
        require_node(owner, record["process"])
        if (type(record["exit"]) is not int or proc.returncode != record["exit"]
                or not all(record["eof"]) or not _same_callable(proc.wait, record["wait"])
                or any(actual is not original for actual, original in zip(
                    (proc.stdout, proc.stderr), record["pipes"]))
                or any(node["methods"][2]() for node in record["readers"])):
            raise OwnedGatewayUnknown("original-probe-exit-and-both-eof-required")
        for channel, stream in enumerate(record["pipes"]):
            if not _same_method(stream, "close", record["close"][channel], record["close_types"][channel]):
                raise OwnedGatewayUnknown("original-probe-pipe-closer-changed")
            record["close"][channel]()
            if stream.closed is not True:
                raise OwnedGatewayUnknown("original-probe-pipe-did-not-close")
        record["finished"] = True
        return (record["exit"], *(bytes(raw).decode("utf-8", "replace").strip() for raw in record["output"]))
    except BaseException as error:
        fail(owner.source, error)
        raise
