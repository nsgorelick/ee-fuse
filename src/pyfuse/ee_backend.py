from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import datetime
from typing import Any

from .backend import Backend
from .cache import TtlCache
from .errors import eacces, eagain, enoent, enotdir, enotsup
from .models import Node, NodePermissions, NodeTimestamps, NodeType
from .paths import normalize_path, split_parent

CATALOG_ROOT_ASSET = "projects/earthengine-public/assets"


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
        self._root_permissions = NodePermissions(
            read=True,
            write_metadata=True,
            write_content=False,
            delete=True,
            share=False,
        )

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

        page_size = max(1, offset + limit)
        try:
            response = self.ee.data.listAssets(
                {"parent": asset_id, "pageSize": page_size, "view": "BASIC"}
            )
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        assets = response.get("assets", []) if isinstance(response, dict) else []
        children: list[Node] = []
        for asset in assets[offset : offset + limit]:
            child_path = self._path_from_asset_id(asset["name"], is_catalog=is_catalog)
            children.append(self._node_from_asset(asset, path=child_path, is_catalog=is_catalog))
        return children

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
        return self._node_from_asset(created, path=target_path, is_catalog=False)

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
        return self._node_from_asset(renamed, path=dest_path, is_catalog=False)

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
        return Node(
            node_type=node_type,
            display_name=_basename(path),
            canonical_path=path,
            stable_id=asset.get("name", path),
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
        params = {"parent": asset_id, "pageSize": max(1, offset + limit), "view": "BASIC"}
        try:
            response = self.ee.data.listImages(params)
        except Exception as exc:
            raise _to_fs_error(exc) from exc
        images = response.get("images", []) if isinstance(response, dict) else []
        taken = images[offset : offset + limit]
        used: dict[str, int] = {}
        members: list[Node] = []
        for image in taken:
            idx = image.get("properties", {}).get("system:index")
            if not idx:
                idx = _basename(image.get("name", ""))
            if not idx:
                digest = hashlib.sha1(image.get("name", "").encode("utf-8")).hexdigest()[:8]
                idx = f"item-{digest}"
            candidate = _safe_name(str(idx))
            count = used.get(candidate, 0)
            used[candidate] = count + 1
            display = candidate if count == 0 else f"{candidate}~{count}"
            child_path = normalize_path(parent.canonical_path.rstrip("/") + "/" + display)
            members.append(
                Node(
                    node_type=NodeType.VIRTUAL_MEMBER,
                    display_name=display,
                    canonical_path=child_path,
                    stable_id=image.get("name", child_path),
                    parent_stable_id=parent.stable_id,
                    permissions=NodePermissions(
                        read=True,
                        write_metadata=not is_catalog,
                        write_content=False,
                        delete=not is_catalog,
                        share=not is_catalog,
                    ),
                    timestamps=NodeTimestamps(
                        created=_parse_time(image.get("createTime")),
                        updated=_parse_time(image.get("updateTime")),
                    ),
                    metadata={
                        "type": "IMAGE",
                        "name": image.get("name"),
                        "properties": image.get("properties", {}),
                    },
                    etag_or_version=image.get("updateTime", "v0"),
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
