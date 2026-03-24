from __future__ import annotations

"""
Earth Engine-backed implementation of the pyfuse Backend protocol.

Caching model in this module:
- `_node_cache`: short-lived cache for resolved nodes addressed by canonical path.
- `_member_node_cache`: short-lived cache for synthesized collection-member nodes.
- `_directory_listing_cache`: directory snapshot cache (path -> child-path map); also
  stores **image collection** members as synthetic `VIRTUAL_MEMBER` nodes so readdir
  does not restart pagination from page 1 for every offset and getattr does not scan
  the whole collection per entry.
- `_directory_listing_versions`: parent asset updateTime associated with snapshot.
- `_directory_sorted_children`: pre-sorted `list[Node]` per parent, updated with each
  `_put_directory_snapshot` so `list_children` slices without re-sorting N keys every call.
- `_directory_member_by_name`: per parent, map ``display_name`` → ``Node`` so FUSE
  ``getattr`` paths that match the listing basename still hit the snapshot when the
  full canonical path string differs slightly from the dict key.
- `_directory_version_probe_cache`: short-lived cache for parent updateTime probes.
- `_catalog_root_cache`: STAC-derived `/ee/catalog` pseudo-root entries.

Design intent:
- Serve repeated getattr/readdir bursts from memory.
- Keep staleness bounded via TTL and version probes.
- Invalidate aggressively after mutating operations.
- Resolve a single asset with `getAsset` when possible; otherwise scan the parent via
  `listAssets` with ``view=FULL`` (no BASIC listing).
  (one API surface, consistent rows for nodes and staleness probes).
- `listAssets` accepts `filter`, but Earth Engine applies it only when the parent is an
  `ImageCollection`; for `Folder` parents the filter is ignored (see REST
  `projects.assets.list`). That is why child lookup under folders still scans pages
  by leaf name.
"""

import hashlib
import json
import logging
import re
import time
from dataclasses import replace
from datetime import datetime
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

from .backend import Backend
from .cache import TtlCache
from .errors import eacces, eagain, enoent, enotdir, enotsup
from .leaf_properties import leaf_properties_payload_bytes
from .models import Node, NodePermissions, NodeTimestamps, NodeType
from .paths import is_fuse_client_sidecar_path, normalize_path, split_parent

CATALOG_ROOT_ASSET = "projects/earthengine-public/assets"
STAC_ROOT_URL = "https://storage.googleapis.com/earthengine-stac/catalog/catalog.json"
# Larger pages mean fewer HTTP round trips for big folders (EE may return fewer than requested).
EE_LIST_ASSETS_PAGE_SIZE = 1000
logger = logging.getLogger(__name__)


def ensure_ee_initialized(
    project: str | None = None,
    authenticate_first: bool = False,
    auth_mode: str | None = None,
) -> Any:
    try:
        import ee  # type: ignore
    except Exception as exc:
        raise enotsup(
            "earthengine-api is not installed; install with `pip install earthengine-api`"
        ) from exc

    try:
        if authenticate_first:
            ee.Authenticate(auth_mode=auth_mode, force=False)
        if project:
            ee.Initialize(project=project)
        else:
            ee.Initialize()
    except Exception as exc:
        raise eacces(
            "failed to initialize Earth Engine credentials. "
            f"Run `earthengine authenticate --force`. Details: {exc}"
        ) from exc
    return ee


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _basename(path: str) -> str:
    return path.rstrip("/").split("/")[-1] if path != "/" else ""


_EE_PROJECT_ASSETS_RE = re.compile(r"^projects/[^/]+/assets/(.+)$")


def _relative_ee_asset_id(name: str) -> str:
    """
    Reduce EE `listAssets` / asset `name` values to the id shape used internally.

    The API often returns resource names such as
    `projects/earthengine-legacy/assets/users/alice/...` while `listAssets`
    calls elsewhere use `users/alice/...`. If we do not normalize, directory
    snapshot keys and `canonical_path` drift from FUSE paths, so getattr
    misses caches and repeats `listAssets` for every directory entry.
    """
    name = name.strip()
    if not name:
        return name
    m = _EE_PROJECT_ASSETS_RE.match(name)
    if m:
        return m.group(1)
    idx = name.find("users/")
    if idx >= 0:
        return name[idx:]
    return name


def _safe_name(candidate: str) -> str:
    return candidate.replace("/", "_").strip() or "item"


def _node_type_from_asset(asset_type: str) -> NodeType:
    mapping = {
        "FOLDER": NodeType.DIRECTORY,
        "IMAGE": NodeType.IMAGE,
        "IMAGE_COLLECTION": NodeType.IMAGE_COLLECTION,
        "TABLE": NodeType.TABLE,
    }
    return mapping.get(asset_type, NodeType.TABLE)


def _to_fs_error(exc: Exception) -> Exception:
    message = str(exc)
    lowered = message.lower()
    if "permission" in lowered or "forbidden" in lowered:
        return eacces(message)
    if "not found" in lowered or "does not exist" in lowered:
        return enoent(message)
    if "rate" in lowered or "quota" in lowered or "tempor" in lowered:
        return eagain(message)
    return enotsup(message)


class EarthEngineBackend(Backend):
    """Backend adapter that maps pyfuse paths to Earth Engine assets."""

    def __init__(
        self,
        project_id: str,
        authenticate_first: bool = False,
        auth_mode: str | None = None,
        legacy_user: str | None = None,
    ) -> None:
        self.project_id = project_id
        self.ee = ensure_ee_initialized(
            project_id,
            authenticate_first=authenticate_first,
            auth_mode=auth_mode,
        )
        self.legacy_user = legacy_user or self._autodetect_legacy_user()
        self._node_cache: TtlCache[Node] = TtlCache(120.0, refresh_on_access=True)
        self._member_node_cache: TtlCache[Node] = TtlCache(120.0, refresh_on_access=True)
        # parent_path -> snapshot used to satisfy lookup/getattr quickly
        self._directory_listing_cache: TtlCache[dict[str, Node]] = TtlCache(
            120.0,
            refresh_on_access=True,
        )
        # parent_path -> display_name -> Node (same entries as snapshot; for getattr basename match)
        self._directory_member_by_name: dict[str, dict[str, Node]] = {}
        # parent_path -> parent asset updateTime used for snapshot validation
        self._directory_listing_versions: dict[str, str | None] = {}
        self._directory_sorted_children: dict[str, list[Node]] = {}
        # parent_path -> recent parent version probe result
        self._directory_version_probe_cache: TtlCache[str | None] = TtlCache(5.0)
        self._catalog_root_cache: TtlCache[list[Node]] = TtlCache(300.0)
        self._root_permissions = NodePermissions(
            read=True,
            write_metadata=True,
            write_content=False,
            delete=True,
            share=False,
        )
        self._warm_catalog_roots()

    def get_node(self, canonical_path: str) -> Node:
        """Resolve one node, preferring warm caches before EE API lookups."""
        path = normalize_path(canonical_path)
        if is_fuse_client_sidecar_path(path):
            raise enoent(f"not an Earth Engine asset path: {path}")
        t0 = time.perf_counter()
        cached = self._node_cache.get(path)
        if cached is not None:
            logger.debug("get_node source=_node_cache path=%s ms=%.1f", path, (time.perf_counter() - t0) * 1000.0)
            return cached
        cached_member = self._member_node_cache.get(path)
        if cached_member is not None:
            logger.debug("get_node source=_member_node_cache path=%s ms=%.1f", path, (time.perf_counter() - t0) * 1000.0)
            return cached_member
        virtual = self._get_virtual_node(path)
        if virtual is not None:
            self._node_cache.put(path, virtual)
            logger.debug("get_node source=virtual path=%s ms=%.1f", path, (time.perf_counter() - t0) * 1000.0)
            return virtual
        snapshot_node = self._node_from_cached_parent_listing(path)
        if snapshot_node is not None:
            self._node_cache.put(path, snapshot_node)
            logger.debug("get_node source=snapshot path=%s ms=%.1f", path, (time.perf_counter() - t0) * 1000.0)
            return snapshot_node

        asset_id, is_catalog = self._asset_id_from_path(path)
        t1 = time.perf_counter()
        try:
            asset = self._get_asset_record_via_parent_listing(asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        t2 = time.perf_counter()
        node = self._node_from_asset(asset, path=path, is_catalog=is_catalog)
        self._node_cache.put(path, node)
        logger.debug(
            "get_node source=parent_listing path=%s asset_id=%s resolve_ms=%.1f node_build_ms=%.1f total_ms=%.1f",
            path,
            asset_id,
            (t2 - t1) * 1000.0,
            (time.perf_counter() - t2) * 1000.0,
            (time.perf_counter() - t0) * 1000.0,
        )
        return node

    def list_children(self, parent_path: str, offset: int, limit: int) -> list[Node]:
        """
        Return directory children with cache-first behavior and version-aware refresh.

        Cache strategy:
        - serve virtual roots directly without API calls
        - serve `/ee/catalog` root from STAC cache
        - use directory snapshot cache for regular directories
        - probe parent updateTime to decide if snapshot is stale
        - refresh snapshot only when needed
        """
        parent_path = normalize_path(parent_path)
        started = time.perf_counter()
        t_pre_parent = time.perf_counter()
        parent = self.get_node(parent_path)
        get_parent_ms = (time.perf_counter() - t_pre_parent) * 1000.0
        if not parent.is_directory_like:
            raise enotdir(f"not a directory-like node: {parent_path}")

        virtual = self._list_virtual_children(parent_path)
        if virtual is not None:
            logger.debug(
                "list_children virtual parent=%s offset=%s limit=%s elapsed_ms=%.1f",
                parent_path,
                offset,
                limit,
                (time.perf_counter() - started) * 1000.0,
            )
            return virtual[offset : offset + limit]
        if parent_path == "/ee/catalog":
            roots = self._catalog_root_cache.get("root")
            if roots is None:
                roots = self._stac_catalog_root_nodes()
                self._catalog_root_cache.put("root", roots)
            for node in roots:
                self._node_cache.put(node.canonical_path, node)
            self._put_directory_snapshot(
                parent_path="/ee/catalog",
                snapshot={node.canonical_path: node for node in roots},
                version=None,
            )
            logger.debug(
                "list_children catalog_root stac_nodes=%s elapsed_ms=%.1f",
                len(roots),
                (time.perf_counter() - started) * 1000.0,
            )
            return roots[offset : offset + limit]

        asset_id, is_catalog = self._asset_id_from_path(parent_path)
        is_collection = parent.node_type == NodeType.IMAGE_COLLECTION

        snapshot = self._directory_listing_cache.get(parent_path)
        if snapshot is None:
            self._directory_sorted_children.pop(parent_path, None)
        snapshot_stale = False
        probe_ms = 0.0
        fetch_ms = 0.0
        if snapshot is not None:
            t_probe = time.perf_counter()
            current_version = self._probe_directory_version(parent_path, asset_id, is_catalog)
            probe_ms += (time.perf_counter() - t_probe) * 1000.0
            cached_version = self._directory_listing_versions.get(parent_path)
            if current_version != cached_version:
                snapshot_stale = True
            logger.debug(
                "list_children cache_check parent=%s hit=true stale=%s cached_ver=%s current_ver=%s probe_ms=%.1f",
                parent_path,
                snapshot_stale,
                cached_version,
                current_version,
                probe_ms,
            )
        else:
            logger.debug("list_children cache_check parent=%s hit=false", parent_path)
        if snapshot is None or snapshot_stale:
            try:
                t_fetch = time.perf_counter()
                if is_collection:
                    snapshot = self._fetch_collection_snapshot(
                        parent_path=parent_path,
                        parent_node=parent,
                        asset_id=asset_id,
                        is_catalog=is_catalog,
                    )
                else:
                    snapshot = self._fetch_directory_snapshot(
                        parent_path=parent_path,
                        asset_id=asset_id,
                        is_catalog=is_catalog,
                    )
                fetch_ms += (time.perf_counter() - t_fetch) * 1000.0
            except Exception as exc:
                # Catalog backend can intermittently fail for some accounts/client
                # combinations; keep mount navigable rather than surfacing EACCES
                # for a read-only optional subtree.
                if is_catalog:
                    snapshot = {}
                else:
                    raise exc
            # _fetch_*_snapshot drains all EE pages, then we slice for offset/limit.
            logger.debug(
                "list_children snapshot_refresh parent=%s children=%s version=%s fetch_ms=%.1f",
                parent_path,
                len(snapshot),
                self._directory_listing_versions.get(parent_path),
                fetch_ms,
            )
        t_sort = time.perf_counter()
        children = self._sorted_children_for_directory(parent_path, snapshot)
        sort_ms = (time.perf_counter() - t_sort) * 1000.0
        logger.debug(
            "list_children done parent=%s offset=%s limit=%s returned=%s "
            "total_ms=%.1f get_parent_ms=%.1f probe_ms=%.1f fetch_ms=%.1f sort_ms=%.1f",
            parent_path,
            offset,
            limit,
            len(children[offset : offset + limit]),
            (time.perf_counter() - started) * 1000.0,
            get_parent_ms,
            probe_ms,
            fetch_ms,
            sort_ms,
        )
        return children[offset : offset + limit]

    def mkdir(self, parent_path: str, name: str) -> Node:
        """Create a folder asset and invalidate parent directory caches."""
        parent_path = normalize_path(parent_path)
        parent = self.get_node(parent_path)
        if not parent.permissions.write_metadata:
            raise eacces(f"mkdir denied on parent: {parent_path}")
        target_path = normalize_path(parent_path.rstrip("/") + "/" + name)
        asset_id, _ = self._asset_id_from_path(target_path)
        try:
            self.ee.data.createAsset({"type": "Folder"}, asset_id)
            created = self._get_asset_record_via_parent_listing(asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        self._invalidate_directory_listing(parent_path)
        node = self._node_from_asset(created, path=target_path, is_catalog=False)
        self._node_cache.put(target_path, node)
        return node

    def rename(self, source_path: str, dest_path: str) -> Node:
        """Rename an asset and invalidate source/destination parent caches."""
        source_path = normalize_path(source_path)
        dest_path = normalize_path(dest_path)
        source_asset_id, source_catalog = self._asset_id_from_path(source_path)
        dest_asset_id, dest_catalog = self._asset_id_from_path(dest_path)
        if source_catalog or dest_catalog:
            raise enotsup("catalog assets are read-only")
        try:
            self.ee.data.renameAsset(source_asset_id, dest_asset_id)
            renamed = self._get_asset_record_via_parent_listing(dest_asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        src_parent = split_parent(source_path)[0]
        dst_parent = split_parent(dest_path)[0]
        self._invalidate_directory_listing(src_parent)
        self._invalidate_directory_listing(dst_parent)
        self._node_cache.invalidate(source_path)
        node = self._node_from_asset(renamed, path=dest_path, is_catalog=False)
        self._node_cache.put(dest_path, node)
        return node

    def unlink(self, path: str) -> None:
        """Delete a non-directory asset and invalidate its parent cache state."""
        path = normalize_path(path)
        node = self.get_node(path)
        if node.is_directory_like:
            raise enotdir("unlink requires non-directory node")
        asset_id, is_catalog = self._asset_id_from_path(path)
        if is_catalog:
            raise enotsup("catalog assets are read-only")
        try:
            self.ee.data.deleteAsset(asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        parent_path = split_parent(path)[0]
        self._invalidate_directory_listing(parent_path)
        self._node_cache.invalidate(path)

    def rmdir(self, path: str) -> None:
        """Delete an empty directory-like asset and invalidate parent cache state."""
        path = normalize_path(path)
        node = self.get_node(path)
        if not node.is_directory_like:
            raise enotdir("rmdir requires directory-like node")
        asset_id, is_catalog = self._asset_id_from_path(path)
        if is_catalog:
            raise enotsup("catalog assets are read-only")
        try:
            children = self.list_children(path, offset=0, limit=1)
            if children:
                raise eacces("directory is not empty")
            self.ee.data.deleteAsset(asset_id)
        except Exception as exc:
            if isinstance(exc, Exception) and exc.__class__.__name__ == "FuseError":
                raise
            raise _to_fs_error(exc) from exc
        parent_path = split_parent(path)[0]
        self._invalidate_directory_listing(parent_path)
        self._node_cache.invalidate(path)

    def update_properties(self, path: str, properties: dict[str, object]) -> Node:
        """Apply EE property updates, then invalidate parent directory caches."""
        path = normalize_path(path)
        node = self.get_node(path)
        if not node.permissions.write_metadata:
            raise eacces(f"property update denied: {path}")
        asset_id, is_catalog = self._asset_id_from_path(path)
        if is_catalog:
            raise enotsup("catalog assets are read-only")
        try:
            self.ee.data.setAssetProperties(asset_id, properties)
            refreshed = self._get_asset_record_via_parent_listing(asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc

        parent_path = split_parent(path)[0]
        self._invalidate_directory_listing(parent_path)
        updated = self._node_from_asset(refreshed, path=path, is_catalog=False)
        self._node_cache.put(path, updated)
        return updated

    def _node_from_asset(self, asset: dict[str, Any], path: str, is_catalog: bool) -> Node:
        """Translate an EE asset dict into a pyfuse Node."""
        asset_type = (asset.get("type") or "").upper()
        node_type = _node_type_from_asset(asset_type)
        timestamps = NodeTimestamps(
            created=_parse_time(asset.get("createTime")),
            updated=_parse_time(asset.get("updateTime")),
        )
        readonly = is_catalog
        permissions = NodePermissions(
            read=True,
            write_metadata=not readonly,
            write_content=False,
            delete=not readonly,
            share=not readonly,
        )
        metadata = {
            "type": asset.get("type"),
            "sizeBytes": asset.get("sizeBytes"),
            "startTime": asset.get("startTime"),
            "endTime": asset.get("endTime"),
            "properties": asset.get("properties", {}),
        }
        stable_id = asset.get("name", path)
        metadata["_properties_size_hint"] = len(leaf_properties_payload_bytes(metadata, stable_id))
        return Node(
            node_type=node_type,
            display_name=_basename(path),
            canonical_path=path,
            stable_id=stable_id,
            parent_stable_id=split_parent(path)[0] if path != "/" else None,
            permissions=permissions,
            timestamps=timestamps,
            metadata=metadata,
            etag_or_version=asset.get("updateTime", "v0"),
        )

    def _get_virtual_node(self, path: str) -> Node | None:
        """Return synthetic mount-layout nodes (`/ee`, `/ee/projects`, etc.)."""
        if path == "/":
            return Node(
                node_type=NodeType.DIRECTORY,
                display_name="",
                canonical_path="/",
                stable_id="root",
                parent_stable_id=None,
                permissions=self._root_permissions,
            )
        if path == "/ee":
            return self._virtual_dir("/ee", parent="root", name="ee")
        if path == "/ee/projects":
            return self._virtual_dir("/ee/projects", parent="/ee", name="projects")
        if path == "/ee/users":
            return self._virtual_dir("/ee/users", parent="/ee", name="users")
        if path == f"/ee/projects/{self.project_id}":
            return self._virtual_dir(path, parent="/ee/projects", name=self.project_id)
        if path == f"/ee/projects/{self.project_id}/assets":
            return self._virtual_dir(path, parent=f"/ee/projects/{self.project_id}", name="assets")
        if self.legacy_user and path == f"/ee/users/{self.legacy_user}":
            return self._virtual_dir(path, parent="/ee/users", name=self.legacy_user)
        if self.legacy_user and path == f"/ee/users/{self.legacy_user}/legacy-assets":
            return self._virtual_dir(
                path,
                parent=f"/ee/users/{self.legacy_user}",
                name="legacy-assets",
            )
        if path == "/ee/catalog":
            node = self._virtual_dir("/ee/catalog", parent="/ee", name="catalog")
            return replace(
                node,
                permissions=NodePermissions(read=True, write_metadata=False, delete=False, share=False),
            )
        return None

    def _virtual_dir(self, path: str, parent: str, name: str) -> Node:
        """Construct a synthetic directory node for the mount namespace."""
        return Node(
            node_type=NodeType.DIRECTORY,
            display_name=name,
            canonical_path=path,
            stable_id=f"virtual:{path}",
            parent_stable_id=parent,
            permissions=self._root_permissions,
            metadata={"virtual": True},
            etag_or_version="virtual-v1",
        )

    def _list_virtual_children(self, parent_path: str) -> list[Node] | None:
        """Return children for synthetic mount-layout directories."""
        if parent_path == "/":
            return [self.get_node("/ee")]
        if parent_path == "/ee":
            children = [self.get_node("/ee/projects"), self.get_node("/ee/catalog")]
            if self.legacy_user:
                children.append(self.get_node("/ee/users"))
            return children
        if parent_path == "/ee/projects":
            return [self.get_node(f"/ee/projects/{self.project_id}")]
        if self.legacy_user and parent_path == "/ee/users":
            return [self.get_node(f"/ee/users/{self.legacy_user}")]
        if self.legacy_user and parent_path == f"/ee/users/{self.legacy_user}":
            return [self.get_node(f"/ee/users/{self.legacy_user}/legacy-assets")]
        if parent_path == f"/ee/projects/{self.project_id}":
            return [self.get_node(f"/ee/projects/{self.project_id}/assets")]
        return None

    def _list_collection_members(self, parent: Node, offset: int, limit: int) -> list[Node]:
        """Delegate to :meth:`list_children` (snapshot-paginated collection listing)."""
        return self.list_children(parent.canonical_path, offset=offset, limit=limit)

    def _asset_id_from_path(self, path: str) -> tuple[str, bool]:
        """Map mount path to Earth Engine asset ID and catalog flag."""
        path = normalize_path(path)
        proj_root = f"/ee/projects/{self.project_id}/assets"
        legacy_root = (
            f"/ee/users/{self.legacy_user}/legacy-assets" if self.legacy_user else None
        )
        if path == "/ee/catalog":
            return CATALOG_ROOT_ASSET, True
        if path.startswith("/ee/catalog/"):
            suffix = path[len("/ee/catalog/") :]
            return f"{CATALOG_ROOT_ASSET}/{suffix}", True
        if legacy_root and path == legacy_root:
            return f"users/{self.legacy_user}", False
        if legacy_root and path.startswith(legacy_root + "/"):
            suffix = path[len(legacy_root + "/") :]
            return f"users/{self.legacy_user}/{suffix}", False
        if path == proj_root:
            return f"projects/{self.project_id}/assets", False
        if path.startswith(proj_root + "/"):
            suffix = path[len(proj_root + "/") :]
            return f"projects/{self.project_id}/assets/{suffix}", False
        raise enoent(f"path is not mapped to an Earth Engine asset: {path}")

    def _path_from_asset_id(self, asset_id: str, is_catalog: bool) -> str:
        """Map Earth Engine asset ID back to mount path."""
        raw = asset_id.strip()
        asset_id = _relative_ee_asset_id(raw)

        if is_catalog:
            if raw == CATALOG_ROOT_ASSET or asset_id == CATALOG_ROOT_ASSET:
                return "/ee/catalog"
            suffix = asset_id.lstrip("/")
            return normalize_path("/ee/catalog/" + suffix) if suffix else "/ee/catalog"

        if self.legacy_user:
            legacy_prefix = f"users/{self.legacy_user}"
            if asset_id == legacy_prefix:
                return f"/ee/users/{self.legacy_user}/legacy-assets"
            if asset_id.startswith(legacy_prefix + "/"):
                suffix = asset_id[len(legacy_prefix) :].lstrip("/")
                return normalize_path(f"/ee/users/{self.legacy_user}/legacy-assets/{suffix}")

        prefix = f"projects/{self.project_id}/assets"
        if asset_id == prefix:
            return f"/ee/projects/{self.project_id}/assets"
        if asset_id.startswith(prefix + "/"):
            suffix = asset_id[len(prefix) + 1 :].lstrip("/")
            return normalize_path(f"/ee/projects/{self.project_id}/assets/{suffix}")
        suffix = asset_id.lstrip("/")
        return normalize_path(f"/ee/projects/{self.project_id}/assets/{suffix}")

    def _autodetect_legacy_user(self) -> str | None:
        """Best-effort detection of legacy `users/<name>` root."""
        # Fast path: many users keep project id == legacy user id.
        try:
            self._get_asset_record_via_parent_listing(f"users/{self.project_id}")
            return self.project_id
        except Exception:
            pass

        # Fallback: inspect legacy root and pick an accessible users/<name> entry.
        try:
            response = self.ee.data.listAssets(
                {
                    "parent": "projects/earthengine-legacy/assets",
                    "pageSize": EE_LIST_ASSETS_PAGE_SIZE,
                    "view": "FULL",
                }
            )
        except Exception:
            return None

        assets = response.get("assets", []) if isinstance(response, dict) else []
        users: list[str] = []
        prefix = "projects/earthengine-legacy/assets/users/"
        for asset in assets:
            name = asset.get("name", "")
            if name.startswith(prefix):
                user = name[len(prefix) :].split("/", 1)[0]
                if user:
                    users.append(user)
        if not users:
            return None
        if self.project_id in users:
            return self.project_id
        return users[0]

    def _node_from_cached_parent_listing(self, path: str) -> Node | None:
        """Resolve a child from the cached parent listing (full path or display_name)."""
        if path == "/":
            return None
        try:
            parent_path, leaf = split_parent(path)
        except Exception:
            return None
        snapshot = self._directory_listing_cache.get(parent_path)
        if snapshot is None:
            return None
        node = snapshot.get(path)
        if node is not None:
            return node
        by_name = self._directory_member_by_name.get(parent_path)
        if by_name is None:
            return None
        return by_name.get(leaf)

    def _invalidate_directory_listing(self, parent_path: str) -> None:
        """Drop cached listing, version metadata, sorted index, and probes."""
        self._directory_listing_cache.invalidate(parent_path)
        self._directory_listing_versions.pop(parent_path, None)
        self._directory_sorted_children.pop(parent_path, None)
        self._directory_member_by_name.pop(parent_path, None)
        self._directory_version_probe_cache.invalidate(parent_path)

    def _fetch_directory_snapshot(
        self,
        parent_path: str,
        asset_id: str,
        is_catalog: bool,
    ) -> dict[str, Node]:
        """
        Drain ``listAssets`` for this parent until there is no ``nextPageToken``,
        then cache the full snapshot. FUSE ``readdir`` paging only slices this
        in-memory result; it is not tied to EE page boundaries.
        """
        started = time.perf_counter()
        snapshot: dict[str, Node] = {}
        page_token: str | None = None

        rounds = 0
        page_size = EE_LIST_ASSETS_PAGE_SIZE
        while True:
            rounds += 1
            if rounds > 5000:
                raise eagain("directory listing exceeded pagination safety limit")
            params: dict[str, Any] = {
                "parent": asset_id,
                "pageSize": page_size,
                "view": "FULL",
            }
            if page_token:
                params["pageToken"] = page_token
            try:
                page_started = time.perf_counter()
                response = self.ee.data.listAssets(params)
                logger.debug(
                    "ee.listAssets parent=%s page_size=%s token=%s had_nodes=%s elapsed_ms=%.1f",
                    asset_id,
                    page_size,
                    "yes" if page_token else "no",
                    len(snapshot),
                    (time.perf_counter() - page_started) * 1000.0,
                )
            except Exception as exc:
                raise _to_fs_error(exc) from exc
            page_items = response.get("assets", []) if isinstance(response, dict) else []
            page_token = response.get("nextPageToken") if isinstance(response, dict) else None
            for asset in page_items:
                child_path = self._path_from_asset_id(asset["name"], is_catalog=is_catalog)
                node = self._node_from_asset(asset, path=child_path, is_catalog=is_catalog)
                snapshot[child_path] = node
                self._node_cache.put(child_path, node)
            if not page_token:
                break

        self._put_directory_snapshot(
            parent_path=parent_path,
            snapshot=snapshot,
            version=self._probe_directory_version(parent_path, asset_id, is_catalog),
        )
        logger.debug(
            "snapshot_built parent=%s assets=%s rounds=%s elapsed_ms=%.1f",
            parent_path,
            len(snapshot),
            rounds,
            (time.perf_counter() - started) * 1000.0,
        )
        return snapshot

    def _virtual_member_node_from_image_row(
        self,
        parent: Node,
        child: dict[str, Any],
        is_catalog: bool,
        used: dict[str, int],
    ) -> tuple[str, Node]:
        properties = child.get("properties", {}) or {}
        idx = properties.get("system:index")
        if not idx:
            idx = _basename(child.get("name", ""))
        if not idx:
            digest = hashlib.sha1(child.get("name", "").encode("utf-8")).hexdigest()[:8]
            idx = f"item-{digest}"
        candidate = _safe_name(str(idx))
        count = used.get(candidate, 0)
        used[candidate] = count + 1
        display = candidate if count == 0 else f"{candidate}~{count}"
        child_path = normalize_path(parent.canonical_path.rstrip("/") + "/" + display)
        stable_id = child.get("name", child_path)
        child_metadata = {
            "type": "IMAGE",
            "name": stable_id,
            "sizeBytes": child.get("sizeBytes"),
            "startTime": child.get("startTime"),
            "endTime": child.get("endTime"),
            "properties": properties,
        }
        node = Node(
            node_type=NodeType.VIRTUAL_MEMBER,
            display_name=display,
            canonical_path=child_path,
            stable_id=stable_id,
            parent_stable_id=parent.stable_id,
            permissions=NodePermissions(
                read=True,
                write_metadata=not is_catalog,
                write_content=False,
                delete=not is_catalog,
                share=not is_catalog,
            ),
            timestamps=NodeTimestamps(
                created=_parse_time(child.get("createTime")) or parent.timestamps.created,
                updated=_parse_time(child.get("updateTime")) or parent.timestamps.updated,
            ),
            metadata={
                **child_metadata,
                "_properties_size_hint": len(leaf_properties_payload_bytes(child_metadata, stable_id)),
            },
            etag_or_version=child.get("updateTime", "v0"),
        )
        return child_path, node

    def _fetch_collection_snapshot(
        self,
        parent_path: str,
        parent_node: Node,
        asset_id: str,
        is_catalog: bool,
    ) -> dict[str, Node]:
        """
        Drain all collection images (``listAssets`` with ``view=FULL``, or ``listImages``
        fallback), cache the full member set, then let ``list_children`` slice by
        offset/limit without mirroring EE pagination to FUSE offsets.
        """
        started = time.perf_counter()
        list_images_mode = False
        snapshot: dict[str, Node] = {}
        page_token: str | None = None
        used: dict[str, int] = {}

        rounds = 0
        page_size = EE_LIST_ASSETS_PAGE_SIZE
        while True:
            rounds += 1
            if rounds > 5000:
                raise eagain("collection listing exceeded pagination safety limit")
            params: dict[str, Any] = {"parent": asset_id, "pageSize": page_size}
            if page_token:
                params["pageToken"] = page_token
            page_started = time.perf_counter()
            try:
                if not list_images_mode:
                    params["view"] = "FULL"
                    response = self.ee.data.listAssets(params)
                    raw_rows = response.get("assets", []) if isinstance(response, dict) else []
                    page_items = [
                        a for a in raw_rows if (a.get("type") or "").upper() == "IMAGE"
                    ]
                else:
                    params["view"] = "FULL"
                    response = self.ee.data.listImages(params)
                    page_items = list(response.get("images", []) if isinstance(response, dict) else [])
            except Exception as exc:
                if not list_images_mode and page_token is None and len(snapshot) == 0:
                    list_images_mode = True
                    continue
                raise _to_fs_error(exc) from exc

            logger.debug(
                "ee.collection_list parent=%s mode=%s page_size=%s token=%s images_in_page=%s "
                "had_members=%s elapsed_ms=%.1f",
                asset_id,
                "listImages" if list_images_mode else "listAssets",
                page_size,
                "yes" if page_token else "no",
                len(page_items),
                len(snapshot),
                (time.perf_counter() - page_started) * 1000.0,
            )

            page_token = response.get("nextPageToken") if isinstance(response, dict) else None
            for row in page_items:
                path, node = self._virtual_member_node_from_image_row(
                    parent_node, row, is_catalog, used
                )
                snapshot[path] = node
                self._node_cache.put(path, node)
                self._member_node_cache.put(path, node)
            if not page_token:
                break

        self._put_directory_snapshot(
            parent_path=parent_path,
            snapshot=snapshot,
            version=self._probe_directory_version(parent_path, asset_id, is_catalog),
        )
        logger.debug(
            "collection_snapshot_built parent=%s members=%s rounds=%s elapsed_ms=%.1f",
            parent_path,
            len(snapshot),
            rounds,
            (time.perf_counter() - started) * 1000.0,
        )
        return snapshot

    def _get_asset_record_via_parent_listing(self, asset_id: str) -> dict[str, Any]:
        """
        Resolve one asset: prefer ``ee.data.getAsset``, else paginated ``listAssets``
        with ``view=FULL`` on the parent. Collection members are matched by API basename
        or by ``properties.system:index`` when the mount path uses listing display names.
        """
        if "/" not in asset_id:
            raise enoent(f"cannot resolve asset without a parent id: {asset_id}")
        parent_id, leaf = asset_id.rsplit("/", 1)
        if not leaf:
            raise enoent(f"invalid asset id: {asset_id}")
        started = time.perf_counter()
        try:
            got = self.ee.data.getAsset(asset_id)
        except Exception:
            got = None
        if isinstance(got, dict) and got.get("name"):
            logger.debug(
                "resolved asset via getAsset id=%s elapsed_ms=%.1f",
                asset_id,
                (time.perf_counter() - started) * 1000.0,
            )
            return got

        page_token: str | None = None
        for _ in range(200):
            params: dict[str, Any] = {
                "parent": parent_id,
                "pageSize": EE_LIST_ASSETS_PAGE_SIZE,
                "view": "FULL",
            }
            if page_token:
                params["pageToken"] = page_token
            try:
                page_started = time.perf_counter()
                response = self.ee.data.listAssets(params)
                logger.debug(
                    "ee.listAssets (resolve child) parent=%s leaf=%s token=%s elapsed_ms=%.1f",
                    parent_id,
                    leaf,
                    "yes" if page_token else "no",
                    (time.perf_counter() - page_started) * 1000.0,
                )
            except Exception as exc:
                raise _to_fs_error(exc) from exc
            for item in response.get("assets", []) if isinstance(response, dict) else []:
                name = item.get("name") or item.get("id") or ""
                nb = name.rstrip("/").split("/")[-1] if name else ""
                rel = _relative_ee_asset_id(name)
                rb = rel.rstrip("/").split("/")[-1] if rel else ""
                props = item.get("properties") or {}
                idx = props.get("system:index")
                if (
                    (nb and nb == leaf)
                    or (rb and rb == leaf)
                    or (idx is not None and str(idx) == leaf)
                ):
                    logger.debug(
                        "resolved asset via parent listing id=%s elapsed_ms=%.1f",
                        asset_id,
                        (time.perf_counter() - started) * 1000.0,
                    )
                    return item
            page_token = response.get("nextPageToken") if isinstance(response, dict) else None
            if not page_token:
                break
        raise enoent(f"asset not found under parent listing: {asset_id}")

    def _sorted_children_for_directory(
        self, parent_path: str, snapshot: dict[str, Node]
    ) -> list[Node]:
        """Return children sorted by display_name (cached when snapshot size matches)."""
        cached = self._directory_sorted_children.get(parent_path)
        if cached is not None and len(cached) == len(snapshot):
            return cached
        ordered = sorted(snapshot.values(), key=lambda n: n.display_name)
        self._directory_sorted_children[parent_path] = ordered
        return ordered

    def _put_directory_snapshot(
        self,
        parent_path: str,
        snapshot: dict[str, Node],
        version: str | None,
    ) -> None:
        """Write directory snapshot + version metadata into cache state."""
        self._directory_listing_cache.put(parent_path, snapshot)
        self._directory_sorted_children[parent_path] = sorted(
            snapshot.values(),
            key=lambda n: n.display_name,
        )
        self._directory_member_by_name[parent_path] = {
            n.display_name: n for n in snapshot.values()
        }
        self._directory_listing_versions[parent_path] = version

    def _probe_directory_version(
        self,
        parent_path: str,
        asset_id: str,
        is_catalog: bool,
    ) -> str | None:
        """Read/cached parent updateTime used to validate snapshot freshness."""
        # Virtual roots do not have a backing EE asset version.
        if parent_path in {"/", "/ee", "/ee/projects", "/ee/users"}:
            return None
        cached = self._directory_version_probe_cache.get(parent_path)
        if cached is not None:
            return cached
        started = time.perf_counter()
        try:
            asset = self._get_asset_record_via_parent_listing(asset_id)
            version = asset.get("updateTime") if isinstance(asset, dict) else None
            logger.debug(
                "version_probe via listAssets parent-of asset=%s elapsed_ms=%.1f update_time=%s",
                asset_id,
                (time.perf_counter() - started) * 1000.0,
                version,
            )
        except Exception:
            # Preserve current behavior on probe failures; do not break listing.
            version = None
        if is_catalog and parent_path == "/ee/catalog":
            # Catalog root is virtualized from STAC/candidates, no stable updateTime.
            version = None
        self._directory_version_probe_cache.put(parent_path, version)
        return version

    def _stac_catalog_root_nodes(self) -> list[Node]:
        """Build `/ee/catalog` pseudo-root nodes from STAC child prefixes."""
        prefixes = self._stac_catalog_root_prefixes()
        if not prefixes:
            return []
        return [self._catalog_root_stub_node(prefix) for prefix in prefixes]

    def _stac_catalog_root_prefixes(self) -> list[str]:
        """Fetch and parse top-level STAC `child` links into prefix names."""
        try:
            with urlopen(STAC_ROOT_URL, timeout=5) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except Exception:
            return []

        links = payload.get("links", []) if isinstance(payload, dict) else []
        prefixes: set[str] = set()
        for link in links:
            if not isinstance(link, dict):
                continue
            if link.get("rel") != "child":
                continue
            href = str(link.get("href", "")).strip()
            title = str(link.get("title", "")).strip()
            prefix = self._catalog_prefix_from_stac_link(href, title)
            if prefix:
                prefixes.add(prefix)

        if not prefixes:
            return []
        return sorted(prefixes)

    def _catalog_root_stub_node(self, prefix: str) -> Node:
        """Create synthetic read-only catalog root node for one STAC prefix."""
        path = f"/ee/catalog/{prefix}"
        return Node(
            node_type=NodeType.DIRECTORY,
            display_name=prefix,
            canonical_path=path,
            stable_id=f"{CATALOG_ROOT_ASSET}/{prefix}",
            parent_stable_id="/ee/catalog",
            permissions=NodePermissions(
                read=True,
                write_metadata=False,
                write_content=False,
                delete=False,
                share=False,
            ),
            metadata={"type": "FOLDER", "source": "stac-root"},
            etag_or_version="stac-root-v1",
        )

    def _catalog_prefix_from_stac_link(self, href: str, title: str) -> str | None:
        """Extract stable catalog prefix from STAC link href/title fields."""
        for candidate in (title, href):
            value = candidate.strip().rstrip("/")
            if not value:
                continue
            if "://" in value:
                parsed = urlparse(value)
                value = parsed.path
            value = value.split("#", 1)[0].split("?", 1)[0].rstrip("/")
            if not value:
                continue
            leaf = value.split("/")[-1]
            if leaf.lower() == "catalog.json":
                parts = [p for p in value.split("/") if p]
                if len(parts) >= 2:
                    leaf = parts[-2]
                else:
                    continue
            if leaf:
                return leaf
        return None

    def _warm_catalog_roots(self) -> None:
        """Preload STAC pseudo-root nodes so first `/ee/catalog` listing is fast."""
        nodes = self._stac_catalog_root_nodes()
        if nodes:
            self._catalog_root_cache.put("root", nodes)
            self._put_directory_snapshot(
                parent_path="/ee/catalog",
                snapshot={node.canonical_path: node for node in nodes},
                version=None,
            )
