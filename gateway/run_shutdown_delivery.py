"""Interpret adapter delivery results without changing shutdown notice policy."""
from typing import Any


def _send_failed(result: Any) -> bool:
    """True when an adapter ``send()`` result explicitly reports failure."""
    return result is not None and getattr(result, "success", True) is False


def _send_error(result: Any) -> str:
    """Error text of a failed ``send()`` result (adapters may omit it)."""
    return getattr(result, "error", "send returned success=False")
