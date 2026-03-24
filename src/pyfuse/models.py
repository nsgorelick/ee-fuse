from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class NodeType(str, Enum):
    DIRECTORY = "directory"
    IMAGE = "image"
    IMAGE_COLLECTION = "image_collection"
    TABLE = "table"
    CATALOG_DATASET = "catalog_dataset"
    VIRTUAL_MEMBER = "virtual_member"

    @property
    def is_directory_like(self) -> bool:
        return self in {
            NodeType.DIRECTORY,
            NodeType.IMAGE_COLLECTION,
        }


@dataclass(frozen=True, slots=True)
class NodePermissions:
    read: bool = True
    write_metadata: bool = False
    write_content: bool = False
    delete: bool = False
    share: bool = False


@dataclass(frozen=True, slots=True)
class NodeTimestamps:
    created: datetime | None = None
    updated: datetime | None = None

    @staticmethod
    def now() -> "NodeTimestamps":
        now = datetime.now(timezone.utc)
        return NodeTimestamps(created=now, updated=now)


@dataclass(frozen=True, slots=True)
class Node:
    node_type: NodeType
    display_name: str
    canonical_path: str
    stable_id: str
    parent_stable_id: str | None
    permissions: NodePermissions = field(default_factory=NodePermissions)
    timestamps: NodeTimestamps = field(default_factory=NodeTimestamps.now)
    metadata: dict[str, Any] = field(default_factory=dict)
    etag_or_version: str = "v0"

    @property
    def is_directory_like(self) -> bool:
        return self.node_type.is_directory_like
