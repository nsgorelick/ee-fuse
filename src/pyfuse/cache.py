from __future__ import annotations

"""
Shared in-memory TTL cache primitives for pyfuse.

Caching usage notes:
- `service.py` uses `TtlCache` for node metadata, directory pages, and leaf
  JSON-size hints to absorb FUSE getattr/readdir/read bursts.
- `ee_backend.py` uses `TtlCache` for EE node snapshots, member nodes,
  directory snapshots, catalog-root snapshots, and short-lived version probes.
- TTLs are intentionally short because EE assets can change outside this
  process; explicit invalidation is still used after local mutating operations.
"""

import time
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class _Entry(Generic[T]):
    """Single cached value plus monotonic expiry timestamp."""

    value: T
    expires_at: float


class TtlCache(Generic[T]):
    """Tiny TTL cache used for metadata/listing snapshots."""

    def __init__(self, ttl_seconds: float, refresh_on_access: bool = False) -> None:
        """Create a TTL cache where each inserted item expires after `ttl_seconds`.

        If ``refresh_on_access`` is True, a successful :meth:`get` slides the expiry
        forward so long ``readdir`` / ``ls`` sessions are not interrupted when a
        listing snapshot would otherwise expire mid-stream (fixed TTL from ``put``
        only).
        """
        self._ttl = ttl_seconds
        self._refresh_on_access = refresh_on_access
        self._entries: dict[str, _Entry[T]] = {}

    def get(self, key: str) -> T | None:
        """Return cached value for `key`, evicting and returning None if expired/missing."""
        entry = self._entries.get(key)
        if entry is None:
            return None
        now = time.monotonic()
        if entry.expires_at < now:
            self._entries.pop(key, None)
            return None
        if self._refresh_on_access:
            entry.expires_at = now + self._ttl
        return entry.value

    def put(self, key: str, value: T) -> None:
        """Store `value` under `key` with a fresh expiry based on the configured TTL."""
        self._entries[key] = _Entry(
            value=value,
            expires_at=time.monotonic() + self._ttl,
        )

    def invalidate(self, key_prefix: str | None = None) -> None:
        """Invalidate all entries, or only keys starting with `key_prefix`."""
        if key_prefix is None:
            self._entries.clear()
            return
        keys = [k for k in self._entries if k.startswith(key_prefix)]
        for key in keys:
            self._entries.pop(key, None)
