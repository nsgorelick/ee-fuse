from __future__ import annotations

import json
import logging
import stat
from dataclasses import asdict
from datetime import datetime
from typing import Any, Iterator

from .backend import Backend
from .cache import TtlCache
from .ee_backend import EE_LIST_ASSETS_PAGE_SIZE
from .errors import eacces, einval, eisdir, enoent, enotdir
from .models import Node, NodeType
from .leaf_properties import leaf_properties_payload_bytes
from .paths import META_FILENAME, is_meta_path, normalize_path, owning_node_path, split_parent

logger = logging.getLogger(__name__)


def _readdir_meta_cookie(page0_len: int) -> int:
    """Cookie for `.meta.json` after the first listing page (cookies 1–2 are . and ..)."""
    return 3 + page0_len


def _readdir_child_cookie(global_idx: int, page0_len: int) -> int:
    """Stable FUSE directory cookie for the global child at index `global_idx`."""
    if global_idx < page0_len:
        return 3 + global_idx
    return 3 + page0_len + 1 + (global_idx - page0_len)


def _readdir_next_child_index_after(after_off: int, page0_len: int) -> int:
    """
    First child index not yet returned to the kernel when the last emitted cookie
    was `after_off` (0 = start of stream).
    """
    meta_ck = 3 + page0_len
    if after_off < 2:
        return 0
    if after_off == 2:
        return 0
    if after_off < 3 + page0_len:
        return after_off - 2
    if after_off == meta_ck:
        return page0_len
    return page0_len + (after_off - meta_ck)


class PyFuseService:
    """
    Service layer that maps FUSE-style operations to a backend API.
    """

    def __init__(
        self,
        backend: Backend,
        metadata_ttl_seconds: float = 20.0,
        listing_ttl_seconds: float = 120.0,
        page_size: int = EE_LIST_ASSETS_PAGE_SIZE,
    ) -> None:
        self._backend = backend
        self._page_size = page_size
        self._node_cache: TtlCache[Node] = TtlCache(
            metadata_ttl_seconds,
            refresh_on_access=True,
        )
        self._listing_cache: TtlCache[list[Node]] = TtlCache(
            listing_ttl_seconds,
            refresh_on_access=True,
        )
        self._listing_page_index: dict[str, dict[str, str]] = {}
        # Path -> serialized property payload size for leaf nodes.
        self._leaf_size_cache: TtlCache[int] = TtlCache(metadata_ttl_seconds)
        # (path, fh) -> writable JSON payload buffer
        self._write_buffers: dict[tuple[str, int], bytearray] = {}

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
                return self._file_stat(
                    size=cached_size,
                    node=node,
                    writable=self._is_properties_writable_leaf(node),
                )
            hint = (node.metadata or {}).get("_properties_size_hint")
            if isinstance(hint, int) and hint >= 0:
                self._leaf_size_cache.put(path, hint)
                return self._file_stat(
                    size=hint,
                    node=node,
                    writable=self._is_properties_writable_leaf(node),
                )
            # Compute once on cold path so clients that trust st_size
            # (and issue zero-byte reads when size==0) still show content.
            computed_size = len(self._leaf_properties_payload(node))
            self._leaf_size_cache.put(path, computed_size)
            return self._file_stat(
                size=computed_size,
                node=node,
                writable=self._is_properties_writable_leaf(node),
            )
        return self._file_stat(size=0, node=node)

    def _listing_page(self, path: str, offset: int, limit: int) -> list[Node]:
        key = f"{path}:{offset}:{limit}"
        cached = self._listing_cache.get(key)
        if cached is None:
            cached = self._backend.list_children(path, offset=offset, limit=limit)
            self._listing_cache.put(key, cached)
            self._reconcile_listing_page(path, offset, limit, cached)
        return cached

    def _stat_for_readdir_entry(self, node: Node) -> dict[str, Any]:
        """Stat dict for readdir filler so `ls -l` can skip per-entry getattr in many cases."""
        if node.is_directory_like:
            return self._dir_stat(node)
        if self._is_properties_readable_leaf(node):
            hint = (node.metadata or {}).get("_properties_size_hint")
            if isinstance(hint, int) and hint >= 0:
                return self._file_stat(
                    size=hint,
                    node=node,
                    writable=self._is_properties_writable_leaf(node),
                )
            return self._file_stat(size=0, node=node, writable=self._is_properties_writable_leaf(node))
        return self._file_stat(size=0, node=node)

    def _meta_sidecar_stat(self, dir_path: str) -> dict[str, Any]:
        owner = self._get_node(dir_path)
        data = self._meta_json(owner)
        return self._file_stat(len(data), node=owner)

    def iter_readdir(
        self, path: str, fh: int, after_off: int
    ) -> Iterator[tuple[str, dict[str, Any] | None, int]]:
        """
        Yield (name, stat_dict | None, cookie) for FUSE readdir with correct cookies.

        fusepy's default bridge ignores the kernel offset and assigns cookie 0 for
        string entries, which breaks large directories: the kernel retries from the
        start and never finishes. Callers should use a FUSE subclass that passes
        `after_off` and skips entries with cookie <= after_off (see fuse_readdir).
        """
        path = normalize_path(path)
        node = self._get_node(path)
        if not node.is_directory_like:
            raise enotdir(f"path is not a directory: {path}")
        if not node.permissions.read:
            raise eacces(f"read denied: {path}")

        logger.debug("iter_readdir path=%s fh=%s after_off=%s", path, fh, after_off)

        if after_off < 1:
            yield (".", self._dir_stat(node), 1)
        if after_off < 2:
            parent_path = "/" if path == "/" else split_parent(path)[0]
            yield ("..", self._dir_stat(self._get_node(parent_path)), 2)

        limit = self._page_size
        page0 = self._listing_page(path, 0, limit)
        page0_len = len(page0)
        meta_ck = _readdir_meta_cookie(page0_len)
        next_g = _readdir_next_child_index_after(after_off, page0_len)

        for i, child in enumerate(page0):
            if i < next_g:
                continue
            ck = _readdir_child_cookie(i, page0_len)
            if ck <= after_off:
                continue
            self._node_cache.put(child.canonical_path, child)
            yield (child.display_name, self._stat_for_readdir_entry(child), ck)

        if meta_ck > after_off:
            yield (META_FILENAME, self._meta_sidecar_stat(path), meta_ck)

        next_idx = page0_len
        while True:
            cached = self._listing_page(path, next_idx, limit)
            if not cached:
                break
            for i, child in enumerate(cached):
                g = next_idx + i
                if g < next_g:
                    continue
                ck = _readdir_child_cookie(g, page0_len)
                if ck <= after_off:
                    continue
                self._node_cache.put(child.canonical_path, child)
                yield (child.display_name, self._stat_for_readdir_entry(child), ck)
            if len(cached) < limit:
                break
            next_idx += limit

    def readdir(self, path: str, offset: int = 0, limit: int | None = None) -> list[str]:
        path = normalize_path(path)
        node = self._get_node(path)
        if not node.is_directory_like:
            raise enotdir(f"path is not a directory: {path}")
        if not node.permissions.read:
            raise eacces(f"read denied: {path}")

        limit = self._page_size if limit is None else limit
        cached = self._listing_page(path, offset, limit)
        for child in cached:
            self._node_cache.put(child.canonical_path, child)

        names = [".", ".."] if offset == 0 else []
        names.extend(child.display_name for child in cached)
        if offset == 0:
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

    def open_for_write(self, path: str, fh: int) -> None:
        path = normalize_path(path)
        if is_meta_path(path):
            raise eacces(f"metadata sidecar is read-only: {path}")
        node = self._get_node(path)
        if not self._is_properties_writable_leaf(node):
            raise eacces(f"write denied: {path}")
        payload = self._leaf_properties_payload(node)
        self._write_buffers[(path, fh)] = bytearray(payload)

    def write(self, path: str, fh: int, offset: int, data: bytes) -> int:
        path = normalize_path(path)
        key = (path, fh)
        buf = self._write_buffers.get(key)
        if buf is None:
            raise enoent(f"no write session for path: {path}")
        if offset < 0:
            raise einval("negative write offset")
        if offset > len(buf):
            buf.extend(b"\x00" * (offset - len(buf)))
        end = offset + len(data)
        if end > len(buf):
            buf.extend(b"\x00" * (end - len(buf)))
        buf[offset:end] = data
        return len(data)

    def truncate(self, path: str, fh: int, length: int) -> None:
        path = normalize_path(path)
        key = (path, fh)
        buf = self._write_buffers.get(key)
        if buf is None:
            raise enoent(f"no write session for path: {path}")
        if length < 0:
            raise einval("negative truncate length")
        if length < len(buf):
            del buf[length:]
        elif length > len(buf):
            buf.extend(b"\x00" * (length - len(buf)))

    def release_write(self, path: str, fh: int) -> None:
        path = normalize_path(path)
        key = (path, fh)
        buf = self._write_buffers.pop(key, None)
        if buf is None:
            return
        payload = bytes(buf)
        updated_props = self._parse_writable_properties_payload(payload)
        current = self._get_node(path)
        current_props = (current.metadata or {}).get("properties", {}) or {}
        if updated_props == current_props:
            return
        self._backend.update_properties(path, updated_props)
        self._invalidate_path(path)

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
    def _is_properties_writable_leaf(node: Node) -> bool:
        if not node.permissions.write_metadata:
            return False
        if node.is_directory_like:
            return False
        return node.node_type in (NodeType.IMAGE, NodeType.TABLE)

    @staticmethod
    def _leaf_properties_payload(node: Node) -> bytes:
        """
        JSON focused on EE asset properties and identity — not pixel/table data.
        Mirrors the hypothesis that `cat <image>` exposes scriptable metadata.
        """
        return leaf_properties_payload_bytes(node.metadata, node.stable_id)

    @staticmethod
    def _parse_writable_properties_payload(payload: bytes) -> dict[str, object]:
        try:
            parsed = json.loads(payload.decode("utf-8"))
        except Exception as exc:
            raise einval(f"invalid JSON payload: {exc}") from exc
        if not isinstance(parsed, dict):
            raise einval("payload must be a JSON object")
        if "properties" not in parsed:
            raise einval("payload must include `properties`")
        props = parsed.get("properties")
        if not isinstance(props, dict):
            raise einval("`properties` must be a JSON object")
        for key in props:
            if not isinstance(key, str):
                raise einval("property keys must be strings")
            if key.startswith("system:"):
                raise eacces("system:* properties are read-only")
        return props


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
        return (json.dumps(body, indent=2, sort_keys=True) + "\n").encode("utf-8")

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
    def _file_stat(
        cls,
        size: int,
        node: Node | None = None,
        writable: bool = False,
    ) -> dict[str, Any]:
        mode_bits = 0o644 if writable else 0o444
        return {
            "st_mode": stat.S_IFREG | mode_bits,
            "st_nlink": 1,
            "st_size": size,
            **cls._time_fields(node),
        }
