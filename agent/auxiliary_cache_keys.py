"""Secret-safe auxiliary credential cache discriminators."""
import hashlib
from typing import Any


class _CallableCacheDiscriminator:
    """Hash a credential callback by identity without exposing its state."""

    __slots__ = ("_callback",)

    def __init__(self, callback: Any) -> None:
        self._callback = callback

    def __hash__(self) -> int:
        return id(self._callback)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _CallableCacheDiscriminator) and self._callback is other._callback

    def __repr__(self) -> str:
        return "<callable-api-key>"


def runtime_cache_discriminator(field: str, value: Any) -> Any:
    """Return a hashable, secret-safe runtime cache-key component."""
    if field == "api_key" and callable(value):
        return _CallableCacheDiscriminator(value)
    if field == "api_key" and isinstance(value, str) and value:
        return ("api-key-digest", hashlib.blake2b(value.encode("utf-8"), digest_size=16).digest())
    return value
