from __future__ import annotations

import json
from typing import Any


def leaf_properties_payload_bytes(metadata: dict[str, Any] | None, stable_id: str) -> bytes:
    """UTF-8 JSON property view for image/table leaves; trailing newline for text tools."""
    md = metadata or {}
    props = md.get("properties")
    if props is None:
        props = {}
    body: dict[str, Any] = {
        "asset_id": stable_id,
        "type": md.get("type"),
        "properties": props,
    }
    for key in ("startTime", "endTime", "sizeBytes"):
        if md.get(key) is not None:
            body[key] = md[key]
    return (json.dumps(body, indent=2, sort_keys=True) + "\n").encode("utf-8")


def leaf_properties_payload_size_hint(metadata: dict[str, Any] | None, stable_id: str) -> int:
    return len(leaf_properties_payload_bytes(metadata, stable_id))
