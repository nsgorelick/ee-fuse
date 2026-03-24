from __future__ import annotations

import json
import stat
from dataclasses import asdict
from datetime import datetime
from typing import Any

from .backend import Backend
from .cache import TtlCache
from .errors import eacces, eisdir, enoent, enotdir
from .models import Node, NodeType
from .paths import META_FILENAME, is_meta_path, normalize_path, owning_node_path


class PyFuseService:
    """
    Service layer that maps FUSE-style operations to a backend API.
    """

    def __init__(
        self,
        backend: Backend,
        metadata_ttl_seconds: float = 20.0,
        listing_ttl_seconds: float = 10.0,
        page_size: int = 256,
    ) -> None:
        self._backend = backend
        self._page_size = page_size
        self._node_cache: TtlCache[Node] = TtlCache(metadata_ttl_seconds)
        self._listing_cache: TtlCache[list[Node]] = TtlCache(listing_ttl_seconds)
        self._listing_page_index: dict[str, dict[str, str]] = {}
        # Path -> serialized property payload size for leaf nodes.
        self._leaf_size_cache: TtlCache[int] = TtlCache(metadata_ttl_seconds)

    def getattr(self, path: str) -> dict[str, Any]:
        path = normalize_path(path)
        if is_meta_path(path):
            owner = self._get_node(owning_node_path(path))
            data = self._meta_json(owner)
            return self._file_stat(len(data), node=owner)

        node = self._get_node(path)
        if node.is_directory_like:
            return self._dir_stat(node=node)
        if self._is_properties_readable_leaf(node):
            cached_size = self._leaf_size_cache.get(path)
            if cached_size is not None:
                return self._file_stat(size=cached_size, node=node)
            hint = (node.metadata or {}).get("_properties_size_hint")
            if isinstance(hint, int) and hint >= 0:
                self._leaf_size_cache.put(path, hint)
                return self._file_stat(size=hint, node=node)
            # Lazy fallback: avoid serializing payload on getattr cold path.
            return self._file_stat(size=0, node=node)
        return self._file_stat(size=0, node=node)

    def readdir(self, path: str, offset: int = 0, limit: int | None = None) -> list[str]:
        path = normalize_path(path)
        node = self._get_node(path)
        if not node.is_directory_like:
            raise enotdir(f"path is not a directory: {path}")
        if not node.permissions.read:
            raise eacces(f"read denied: {path}")

        limit = self._page_size if limit is None else limit
        key = f"{path}:{offset}:{limit}"
        cached = self._listing_cache.get(key)
        if cached is None:
            cached = self._backend.list_children(path, offset=offset, limit=limit)
            self._listing_cache.put(key, cached)
            self._reconcile_listing_page(path, offset, limit, cached)
        # Warm node cache with this snapshot so follow-up getattr/stat calls
        # from `ls -l` do not trigger a backend request per entry.
        for child in cached:
            self._node_cache.put(child.canonical_path, child)

        names = [".", ".."] if offset == 0 else []
        names.extend(child.display_name for child in cached)
        names.append(META_FILENAME)
        return names

    def open_for_read(self, path: str) -> None:
        path = normalize_path(path)
        if is_meta_path(path):
            owner = self._get_node(owning_node_path(path))
            if not owner.permissions.read:
                raise eacces(f"read denied: {path}")
            return

        node = self._get_node(path)
        if node.is_directory_like:
            raise eisdir(f"cannot open directory for read: {path}")
        if not node.permissions.read:
            raise eacces(f"read denied: {path}")

    def read(self, path: str, size: int, offset: int) -> bytes:
        path = normalize_path(path)
        if is_meta_path(path):
            owner = self._get_node(owning_node_path(path))
            payload = self._meta_json(owner)
            return payload[offset : offset + size]
        node = self._get_node(path)
        if self._is_properties_readable_leaf(node):
            payload = self._leaf_properties_payload(node)
            self._leaf_size_cache.put(path, len(payload))
            return payload[offset : offset + size]
        raise enoent("read not supported for this path")

    def mkdir(self, path: str) -> Node:
        path = normalize_path(path)
        parent_path, name = path.rsplit("/", 1)
        parent_path = parent_path or "/"
        node = self._backend.mkdir(parent_path, name)
        self._invalidate_path(path)
        return node

    def rename(self, source: str, destination: str) -> Node:
        source = normalize_path(source)
        destination = normalize_path(destination)
        node = self._backend.rename(source, destination)
        self._invalidate_path(source)
        self._invalidate_path(destination)
        return node

    def unlink(self, path: str) -> None:
        path = normalize_path(path)
        node = self._get_node(path)
        if node.is_directory_like:
            raise enotdir("unlink called on directory-like node")
        self._backend.unlink(path)
        self._invalidate_path(path)

    def rmdir(self, path: str) -> None:
        path = normalize_path(path)
        node = self._get_node(path)
        if not node.is_directory_like:
            raise enotdir("rmdir called on non-directory-like node")
        self._backend.rmdir(path)
        self._invalidate_path(path)

    def _get_node(self, canonical_path: str) -> Node:
        canonical_path = normalize_path(canonical_path)
        node = self._node_cache.get(canonical_path)
        if node is None:
            node = self._backend.get_node(canonical_path)
            self._node_cache.put(canonical_path, node)
        return node

    def _invalidate_path(self, path: str) -> None:
        path = normalize_path(path)
        self._node_cache.invalidate(path)
        self._listing_cache.invalidate(path)
        parent = path.rsplit("/", 1)[0] or "/"
        self._listing_cache.invalidate(parent)
        self._leaf_size_cache.invalidate(path)
        stale_pages = [k for k in self._listing_page_index if k.startswith(path) or k.startswith(parent)]
        for stale in stale_pages:
            self._listing_page_index.pop(stale, None)

    def _reconcile_listing_page(
        self,
        path: str,
        offset: int,
        limit: int,
        children: list[Node],
    ) -> None:
        """
        Compare fresh listing page against prior snapshot and invalidate stale
        node cache entries when child etag/updateTime changed or disappeared.
        """
        page_key = f"{path}:{offset}:{limit}"
        previous = self._listing_page_index.get(page_key, {})
        current = {child.canonical_path: child.etag_or_version for child in children}

        # Child disappeared from this page snapshot.
        for child_path in previous:
            if child_path not in current:
                self._node_cache.invalidate(child_path)

        # Child exists but changed version/updateTime.
        for child_path, etag in current.items():
            if previous.get(child_path) not in {None, etag}:
                self._node_cache.invalidate(child_path)

        self._listing_page_index[page_key] = current

    @staticmethod
    def _is_properties_readable_leaf(node: Node) -> bool:
        """Leaf nodes whose primary bytes are a JSON property view (cat-friendly)."""
        if node.is_directory_like:
            return False
        return node.node_type in (
            NodeType.IMAGE,
            NodeType.TABLE,
            NodeType.VIRTUAL_MEMBER,
            NodeType.CATALOG_DATASET,
        )

    @staticmethod
    def _leaf_properties_payload(node: Node) -> bytes:
        """
        JSON focused on EE asset properties and identity — not pixel/table data.
        Mirrors the hypothesis that `cat <image>` exposes scriptable metadata.
        """
        md = node.metadata or {}
        props = md.get("properties")
        if props is None:
            props = {}
        body: dict[str, Any] = {
            "asset_id": node.stable_id,
            "type": md.get("type"),
            "properties": props,
        }
        for key in ("startTime", "endTime", "sizeBytes"):
            if md.get(key) is not None:
                body[key] = md[key]
        return json.dumps(body, indent=2, sort_keys=True).encode("utf-8")

    @staticmethod
    def _metadata_size_bytes(node: Node) -> int | None:
        value = (node.metadata or {}).get("sizeBytes")
        if value is None:
            return None
        if isinstance(value, int):
            return value if value >= 0 else None
        if isinstance(value, str):
            try:
                parsed = int(value)
            except ValueError:
                return None
            return parsed if parsed >= 0 else None
        return None


    @staticmethod
    def _meta_json(node: Node) -> bytes:
        body = {
            "node_type": node.node_type.value,
            "display_name": node.display_name,
            "canonical_path": node.canonical_path,
            "stable_id": node.stable_id,
            "parent_stable_id": node.parent_stable_id,
            "permissions": asdict(node.permissions),
            "timestamps": {
                "created": node.timestamps.created.isoformat() if node.timestamps.created else None,
                "updated": node.timestamps.updated.isoformat() if node.timestamps.updated else None,
            },
            "metadata": node.metadata,
            "etag_or_version": node.etag_or_version,
        }
        return json.dumps(body, indent=2, sort_keys=True).encode("utf-8")

    @staticmethod
    def _to_epoch(value: datetime | None) -> float:
        if value is None:
            return 0.0
        return value.timestamp()

    @classmethod
    def _time_fields(cls, node: Node | None) -> dict[str, float]:
        if node is None:
            return {"st_atime": 0.0, "st_mtime": 0.0, "st_ctime": 0.0}
        mtime = cls._to_epoch(node.timestamps.updated)
        ctime = cls._to_epoch(node.timestamps.created)
        atime = mtime if mtime else ctime
        return {
            "st_atime": atime,
            "st_mtime": mtime,
            "st_ctime": ctime,
        }

    @classmethod
    def _dir_stat(cls, node: Node | None = None) -> dict[str, Any]:
        return {
            "st_mode": stat.S_IFDIR | 0o555,
            "st_nlink": 2,
            "st_size": 0,
            **cls._time_fields(node),
        }

    @classmethod
    def _file_stat(cls, size: int, node: Node | None = None) -> dict[str, Any]:
        return {
            "st_mode": stat.S_IFREG | 0o444,
            "st_nlink": 1,
            "st_size": size,
            **cls._time_fields(node),
        }
