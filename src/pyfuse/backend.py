from __future__ import annotations

from dataclasses import replace
from typing import Protocol

from .errors import eacces, eexist, enoent, enotdir
from .models import Node, NodePermissions, NodeType
from .paths import normalize_path, split_parent


class Backend(Protocol):
    def get_node(self, canonical_path: str) -> Node: ...

    def list_children(self, parent_path: str, offset: int, limit: int) -> list[Node]: ...

    def mkdir(self, parent_path: str, name: str) -> Node: ...

    def rename(self, source_path: str, dest_path: str) -> Node: ...

    def unlink(self, path: str) -> None: ...

    def rmdir(self, path: str) -> None: ...

    def update_properties(self, path: str, properties: dict[str, object]) -> Node: ...


class InMemoryBackend(Backend):
    """Simple backend for local development and tests."""

    def __init__(self, nodes: list[Node]) -> None:
        self._nodes: dict[str, Node] = {normalize_path(n.canonical_path): n for n in nodes}

    def get_node(self, canonical_path: str) -> Node:
        path = normalize_path(canonical_path)
        node = self._nodes.get(path)
        if node is None:
            raise enoent(f"node not found: {path}")
        return node

    def list_children(self, parent_path: str, offset: int, limit: int) -> list[Node]:
        parent = self.get_node(parent_path)
        if not parent.is_directory_like:
            raise enotdir(f"not a directory-like node: {parent_path}")

        prefix = normalize_path(parent_path).rstrip("/")
        prefix = "/" if prefix == "" else prefix
        children = []
        for path, node in self._nodes.items():
            if path == prefix:
                continue
            if path == "/":
                continue
            parent_candidate, _ = split_parent(path)
            if parent_candidate == prefix:
                children.append(node)
        children.sort(key=lambda n: n.display_name)
        return children[offset : offset + limit]

    def mkdir(self, parent_path: str, name: str) -> Node:
        parent = self.get_node(parent_path)
        if not parent.permissions.write_metadata:
            raise eacces(f"mkdir denied on parent: {parent_path}")
        if not parent.is_directory_like:
            raise enotdir(f"not a directory-like node: {parent_path}")
        target = normalize_path(parent.canonical_path.rstrip("/") + "/" + name)
        if target in self._nodes:
            raise eexist(f"target exists: {target}")
        node = Node(
            node_type=NodeType.DIRECTORY,
            display_name=name,
            canonical_path=target,
            stable_id=f"node:{target}",
            parent_stable_id=parent.stable_id,
            permissions=NodePermissions(read=True, write_metadata=True, delete=True),
            metadata={},
            etag_or_version="v1",
        )
        self._nodes[target] = node
        return node

    def rename(self, source_path: str, dest_path: str) -> Node:
        source_path = normalize_path(source_path)
        dest_path = normalize_path(dest_path)
        source = self.get_node(source_path)
        if not source.permissions.write_metadata:
            raise eacces(f"rename denied on node: {source_path}")
        if dest_path in self._nodes:
            raise eexist(f"destination exists: {dest_path}")
        if source.is_directory_like and (dest_path == source_path or dest_path.startswith(source_path + "/")):
            raise eacces(f"cannot move directory into itself: {source_path} -> {dest_path}")
        parent_path, leaf = split_parent(dest_path)
        self.get_node(parent_path)
        if not source.is_directory_like:
            updated = replace(
                source,
                canonical_path=dest_path,
                display_name=leaf,
                etag_or_version=f"{source.etag_or_version}-renamed",
            )
            self._nodes.pop(source_path, None)
            self._nodes[dest_path] = updated
            return updated

        # Move the full subtree when renaming directories.
        impacted = {
            path: node
            for path, node in self._nodes.items()
            if path == source_path or path.startswith(source_path + "/")
        }
        for old_path in impacted:
            new_path = dest_path + old_path[len(source_path) :]
            if new_path not in impacted and new_path in self._nodes:
                raise eexist(f"destination exists: {new_path}")

        moved_by_new_path: dict[str, Node] = {}
        old_to_new: dict[str, str] = {}
        for old_path, node in impacted.items():
            new_path = dest_path + old_path[len(source_path) :]
            old_to_new[old_path] = new_path
            moved_by_new_path[new_path] = replace(
                node,
                canonical_path=new_path,
                display_name=leaf if old_path == source_path else _basename(new_path),
                etag_or_version=f"{node.etag_or_version}-renamed" if old_path == source_path else node.etag_or_version,
            )

        # Rewire parent_stable_id for moved descendants when parent is moved too.
        for new_path, node in list(moved_by_new_path.items()):
            if new_path == dest_path:
                continue
            parent_of_new, _ = split_parent(new_path)
            parent_node = moved_by_new_path.get(parent_of_new)
            if parent_node is not None:
                moved_by_new_path[new_path] = replace(node, parent_stable_id=parent_node.stable_id)

        for old_path in impacted:
            self._nodes.pop(old_path, None)
        self._nodes.update(moved_by_new_path)
        return moved_by_new_path[dest_path]

    def unlink(self, path: str) -> None:
        node = self.get_node(path)
        if node.is_directory_like:
            raise enotdir("unlink requires non-directory node")
        if not node.permissions.delete:
            raise eacces(f"delete denied: {path}")
        self._nodes.pop(normalize_path(path), None)

    def rmdir(self, path: str) -> None:
        node = self.get_node(path)
        if not node.is_directory_like:
            raise enotdir("rmdir requires directory-like node")
        if not node.permissions.delete:
            raise eacces(f"delete denied: {path}")
        if self.list_children(path, offset=0, limit=1):
            raise eacces("directory not empty")
        self._nodes.pop(normalize_path(path), None)

    def update_properties(self, path: str, properties: dict[str, object]) -> Node:
        node = self.get_node(path)
        if not node.permissions.write_metadata:
            raise eacces(f"property update denied: {path}")
        updated_meta = {**(node.metadata or {}), "properties": dict(properties)}
        updated = replace(
            node,
            metadata=updated_meta,
            etag_or_version=f"{node.etag_or_version}-props",
        )
        self._nodes[normalize_path(path)] = updated
        return updated


def _basename(path: str) -> str:
    return path.rstrip("/").split("/")[-1] if path != "/" else ""
