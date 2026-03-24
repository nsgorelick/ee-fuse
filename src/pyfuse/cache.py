from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class _Entry(Generic[T]):
    value: T
    expires_at: float


class TtlCache(Generic[T]):
    """Tiny TTL cache used for metadata/listing snapshots."""

    def __init__(self, ttl_seconds: float) -> None:
        self._ttl = ttl_seconds
        self._entries: dict[str, _Entry[T]] = {}

    def get(self, key: str) -> T | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        if entry.expires_at < time.monotonic():
            self._entries.pop(key, None)
            return None
        return entry.value

    def put(self, key: str, value: T) -> None:
        self._entries[key] = _Entry(
            value=value,
            expires_at=time.monotonic() + self._ttl,
        )

    def invalidate(self, key_prefix: str | None = None) -> None:
        if key_prefix is None:
            self._entries.clear()
            return
        keys = [k for k in self._entries if k.startswith(key_prefix)]
        for key in keys:
            self._entries.pop(key, None)
