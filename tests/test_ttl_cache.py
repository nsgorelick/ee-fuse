from __future__ import annotations

import time

from pyfuse.cache import TtlCache


def test_ttl_cache_refresh_on_access_extends_lifetime() -> None:
    c = TtlCache[str](0.2, refresh_on_access=True)
    c.put("k", "v")
    time.sleep(0.15)
    assert c.get("k") == "v"
    time.sleep(0.15)
    assert c.get("k") == "v"
    time.sleep(0.25)
    assert c.get("k") is None


def test_ttl_cache_without_refresh_expires() -> None:
    c = TtlCache[str](0.15, refresh_on_access=False)
    c.put("k", "v")
    time.sleep(0.2)
    assert c.get("k") is None
