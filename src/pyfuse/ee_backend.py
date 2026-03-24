from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import datetime
from typing import Any
from urllib.parse import urlparse
from urllib.request import urlopen

from .backend import Backend
from .cache import TtlCache
from .errors import eacces, eagain, enoent, enotdir, enotsup
from .models import Node, NodePermissions, NodeTimestamps, NodeType
from .paths import normalize_path, split_parent

CATALOG_ROOT_ASSET = "projects/earthengine-public/assets"
STAC_ROOT_URL = "https://storage.googleapis.com/earthengine-stac/catalog/catalog.json"
CATALOG_ROOT_PREFIX_CANDIDATES = (
    "AHN",
    "ASTER",
    "BIOPAMA",
    "BLM",
    "CAS",
    "COPERNICUS",
    "CSIRO",
    "ESA",
    "ECMWF",
    "FAO",
    "Finland",
    "FIRMS",
    "GLCF",
    "GOOGLE",
    "GRIDMET",
    "IDAHO_EPSCOR",
    "JAXA",
    "LANDSAT",
    "MERIT",
    "MODIS",
    "NASA",
    "NOAA",
    "NRCan",
    "OSM",
    "Oxford",
    "RESOLVE",
    "TIGER",
    "UCSB",
    "UMD",
    "USDA",
    "USGS",
    "WWF",
)


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


def _properties_size_hint(metadata: dict[str, Any], stable_id: str) -> int:
    props = metadata.get("properties", {}) or {}
    body: dict[str, Any] = {
        "asset_id": stable_id,
        "type": metadata.get("type"),
        "properties": props,
    }
    for key in ("startTime", "endTime", "sizeBytes"):
        if metadata.get(key) is not None:
            body[key] = metadata[key]
    return len(json.dumps(body, indent=2, sort_keys=True).encode("utf-8"))


class EarthEngineBackend(Backend):
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
        self._node_cache: TtlCache[Node] = TtlCache(30.0)
        self._member_node_cache: TtlCache[Node] = TtlCache(30.0)
        # parent_path -> snapshot used to satisfy lookup/getattr quickly
        self._directory_listing_cache: TtlCache[dict[str, Node]] = TtlCache(15.0)
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
        path = normalize_path(canonical_path)
        cached = self._node_cache.get(path)
        if cached is not None:
            return cached
        cached_member = self._member_node_cache.get(path)
        if cached_member is not None:
            return cached_member
        virtual = self._get_virtual_node(path)
        if virtual is not None:
            self._node_cache.put(path, virtual)
            return virtual
        virtual_member = self._virtual_member_from_path(path)
        if virtual_member is not None:
            self._member_node_cache.put(path, virtual_member)
            return virtual_member
        snapshot_node = self._node_from_cached_parent_listing(path)
        if snapshot_node is not None:
            self._node_cache.put(path, snapshot_node)
            return snapshot_node

        asset_id, is_catalog = self._asset_id_from_path(path)
        try:
            asset = self.ee.data.getAsset(asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        node = self._node_from_asset(asset, path=path, is_catalog=is_catalog)
        self._node_cache.put(path, node)
        return node

    def list_children(self, parent_path: str, offset: int, limit: int) -> list[Node]:
        parent_path = normalize_path(parent_path)
        parent = self.get_node(parent_path)
        if not parent.is_directory_like:
            raise enotdir(f"not a directory-like node: {parent_path}")

        virtual = self._list_virtual_children(parent_path)
        if virtual is not None:
            return virtual[offset : offset + limit]

        asset_id, is_catalog = self._asset_id_from_path(parent_path)
        if parent.node_type == NodeType.IMAGE_COLLECTION:
            return self._list_collection_members(parent, offset=offset, limit=limit)

        snapshot = self._directory_listing_cache.get(parent_path)
        if snapshot is None:
            try:
                snapshot = self._fetch_directory_snapshot(
                    parent_path=parent_path,
                    asset_id=asset_id,
                    is_catalog=is_catalog,
                    min_count=offset + limit,
                )
            except Exception as exc:
                # Catalog backend can intermittently fail for some accounts/client
                # combinations; keep mount navigable rather than surfacing EACCES
                # for a read-only optional subtree.
                if is_catalog:
                    if parent_path == "/ee/catalog":
                        return self._fallback_catalog_root_listing(offset, limit)
                    snapshot = {}
                else:
                    raise exc
            self._directory_listing_cache.put(parent_path, snapshot)
        children = sorted(snapshot.values(), key=lambda n: n.display_name)
        if len(children) < offset + limit:
            try:
                snapshot = self._fetch_directory_snapshot(
                    parent_path=parent_path,
                    asset_id=asset_id,
                    is_catalog=is_catalog,
                    min_count=offset + limit,
                )
            except Exception as exc:
                if is_catalog:
                    if parent_path == "/ee/catalog":
                        return self._fallback_catalog_root_listing(offset, limit)
                    snapshot = {}
                else:
                    raise exc
            self._directory_listing_cache.put(parent_path, snapshot)
            children = sorted(snapshot.values(), key=lambda n: n.display_name)
        return children[offset : offset + limit]

    def mkdir(self, parent_path: str, name: str) -> Node:
        parent_path = normalize_path(parent_path)
        parent = self.get_node(parent_path)
        if not parent.permissions.write_metadata:
            raise eacces(f"mkdir denied on parent: {parent_path}")
        target_path = normalize_path(parent_path.rstrip("/") + "/" + name)
        asset_id, _ = self._asset_id_from_path(target_path)
        try:
            self.ee.data.createAsset({"type": "Folder"}, asset_id)
            created = self.ee.data.getAsset(asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        self._directory_listing_cache.invalidate(parent_path)
        node = self._node_from_asset(created, path=target_path, is_catalog=False)
        self._node_cache.put(target_path, node)
        return node

    def rename(self, source_path: str, dest_path: str) -> Node:
        source_path = normalize_path(source_path)
        dest_path = normalize_path(dest_path)
        source_asset_id, source_catalog = self._asset_id_from_path(source_path)
        dest_asset_id, dest_catalog = self._asset_id_from_path(dest_path)
        if source_catalog or dest_catalog:
            raise enotsup("catalog assets are read-only")
        try:
            self.ee.data.renameAsset(source_asset_id, dest_asset_id)
            renamed = self.ee.data.getAsset(dest_asset_id)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        src_parent = split_parent(source_path)[0]
        dst_parent = split_parent(dest_path)[0]
        self._directory_listing_cache.invalidate(src_parent)
        self._directory_listing_cache.invalidate(dst_parent)
        self._node_cache.invalidate(source_path)
        node = self._node_from_asset(renamed, path=dest_path, is_catalog=False)
        self._node_cache.put(dest_path, node)
        return node

    def unlink(self, path: str) -> None:
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
        self._directory_listing_cache.invalidate(parent_path)
        self._node_cache.invalidate(path)

    def rmdir(self, path: str) -> None:
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
        self._directory_listing_cache.invalidate(parent_path)
        self._node_cache.invalidate(path)

    def _node_from_asset(self, asset: dict[str, Any], path: str, is_catalog: bool) -> Node:
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
        metadata["_properties_size_hint"] = _properties_size_hint(metadata, stable_id)
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
        asset_id, is_catalog = self._asset_id_from_path(parent.canonical_path)
        page_size = max(1, offset + limit)
        taken: list[dict[str, Any]] = []
        # Prefer listAssets(FULL) because it can include richer per-image fields
        # such as updateTime and sizeBytes for collection children.
        try:
            response = self.ee.data.listAssets(
                {"parent": asset_id, "pageSize": page_size, "view": "FULL"}
            )
            assets = response.get("assets", []) if isinstance(response, dict) else []
            taken = [a for a in assets if (a.get("type") or "").upper() == "IMAGE"][offset : offset + limit]
        except Exception:
            # Fallback for environments where listAssets on collection parents is
            # not available/compatible: use listImages(FULL) and normalize.
            params = {"parent": asset_id, "pageSize": page_size, "view": "FULL"}
            try:
                response = self.ee.data.listImages(params)
            except Exception as exc:
                raise _to_fs_error(exc) from exc
            images = response.get("images", []) if isinstance(response, dict) else []
            taken = images[offset : offset + limit]

        used: dict[str, int] = {}
        members: list[Node] = []
        for child in taken:
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
            members.append(
                Node(
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
                        "_properties_size_hint": _properties_size_hint(child_metadata, stable_id),
                    },
                    etag_or_version=child.get("updateTime", "v0"),
                )
            )
            self._member_node_cache.put(child_path, members[-1])
        return members

    def _asset_id_from_path(self, path: str) -> tuple[str, bool]:
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
        if is_catalog:
            prefix = CATALOG_ROOT_ASSET
            if asset_id == prefix:
                return "/ee/catalog"
            suffix = asset_id[len(prefix) :].lstrip("/")
            return normalize_path("/ee/catalog/" + suffix)
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
        suffix = asset_id[len(prefix) :].lstrip("/")
        return normalize_path(f"/ee/projects/{self.project_id}/assets/{suffix}")

    def _autodetect_legacy_user(self) -> str | None:
        # Fast path: many users keep project id == legacy user id.
        try:
            self.ee.data.getAsset(f"users/{self.project_id}")
            return self.project_id
        except Exception:
            pass

        # Fallback: inspect legacy root and pick an accessible users/<name> entry.
        try:
            response = self.ee.data.listAssets(
                {
                    "parent": "projects/earthengine-legacy/assets",
                    "pageSize": 500,
                    "view": "BASIC",
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
        if path == "/":
            return None
        try:
            parent_path, _ = split_parent(path)
        except Exception:
            return None
        snapshot = self._directory_listing_cache.get(parent_path)
        if snapshot is None:
            return None
        return snapshot.get(path)

    def _fetch_directory_snapshot(
        self,
        parent_path: str,
        asset_id: str,
        is_catalog: bool,
        min_count: int,
    ) -> dict[str, Node]:
        items: list[dict[str, Any]] = []
        page_token: str | None = None
        wanted = max(1, min_count)
        # Pull enough entries for requested window; cap loops defensively.
        for _ in range(20):
            params: dict[str, Any] = {"parent": asset_id, "pageSize": max(256, wanted), "view": "BASIC"}
            if page_token:
                params["pageToken"] = page_token
            try:
                response = self.ee.data.listAssets(params)
            except Exception as exc:
                raise _to_fs_error(exc) from exc
            page_items = response.get("assets", []) if isinstance(response, dict) else []
            items.extend(page_items)
            page_token = response.get("nextPageToken") if isinstance(response, dict) else None
            if len(items) >= wanted or not page_token:
                break

        snapshot: dict[str, Node] = {}
        for asset in items:
            child_path = self._path_from_asset_id(asset["name"], is_catalog=is_catalog)
            node = self._node_from_asset(asset, path=child_path, is_catalog=is_catalog)
            snapshot[child_path] = node
            self._node_cache.put(child_path, node)
        self._directory_listing_cache.put(parent_path, snapshot)
        return snapshot

    def _virtual_member_from_path(self, path: str) -> Node | None:
        if path == "/":
            return None
        try:
            parent_path, leaf = split_parent(path)
        except Exception:
            return None
        if not leaf:
            return None
        try:
            parent = self.get_node(parent_path)
        except Exception:
            return None
        if parent.node_type != NodeType.IMAGE_COLLECTION:
            return None
        _, is_catalog = self._asset_id_from_path(parent_path)
        permissions = NodePermissions(
            read=True,
            write_metadata=not is_catalog,
            write_content=False,
            delete=not is_catalog,
            share=not is_catalog,
        )
        return Node(
            node_type=NodeType.VIRTUAL_MEMBER,
            display_name=leaf,
            canonical_path=path,
            stable_id=f"virtual-member:{path}",
            parent_stable_id=parent.stable_id,
            permissions=permissions,
            metadata={"name_hint": leaf},
            etag_or_version=parent.etag_or_version,
        )

    def _fallback_catalog_root_listing(self, offset: int, limit: int) -> list[Node]:
        cached = self._catalog_root_cache.get("root")
        if cached is None:
            stac_nodes = self._stac_catalog_root_nodes()
            if stac_nodes:
                self._catalog_root_cache.put("root", stac_nodes)
                self._directory_listing_cache.put(
                    "/ee/catalog", {node.canonical_path: node for node in stac_nodes}
                )
                cached = stac_nodes
            else:
                cached = None
        if cached is None:
            nodes: list[Node] = []
            for prefix in CATALOG_ROOT_PREFIX_CANDIDATES:
                asset_id = f"{CATALOG_ROOT_ASSET}/{prefix}"
                try:
                    asset = self.ee.data.getAsset(asset_id)
                except Exception:
                    continue
                path = f"/ee/catalog/{prefix}"
                node = self._node_from_asset(asset, path=path, is_catalog=True)
                self._node_cache.put(path, node)
                nodes.append(node)
            nodes.sort(key=lambda n: n.display_name)
            self._catalog_root_cache.put("root", nodes)
            self._directory_listing_cache.put(
                "/ee/catalog", {node.canonical_path: node for node in nodes}
            )
            cached = nodes
        else:
            # Keep node + directory snapshots warm so getattr/lookups from
            # immediate ls -l calls are served from cache.
            for node in cached:
                self._node_cache.put(node.canonical_path, node)
            self._directory_listing_cache.put(
                "/ee/catalog", {node.canonical_path: node for node in cached}
            )
        return cached[offset : offset + limit]

    def _stac_catalog_root_nodes(self) -> list[Node]:
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

        nodes: list[Node] = []
        for prefix in sorted(prefixes):
            path = f"/ee/catalog/{prefix}"
            try:
                node = self.get_node(path)
            except Exception:
                continue
            nodes.append(node)
        return nodes

    def _catalog_prefix_from_stac_link(self, href: str, title: str) -> str | None:
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
        nodes = self._stac_catalog_root_nodes()
        if nodes:
            self._catalog_root_cache.put("root", nodes)
            self._directory_listing_cache.put(
                "/ee/catalog", {node.canonical_path: node for node in nodes}
            )
