from __future__ import annotations

from pyfuse.backend import InMemoryBackend
from pyfuse.leaf_properties import leaf_properties_payload_size_hint
from pyfuse.errors import FuseError
from pyfuse.models import Node, NodePermissions, NodeType
from pyfuse.paths import is_fuse_client_sidecar_path, normalize_path
from pyfuse.service import PyFuseService


def _seed_nodes() -> list[Node]:
    writable = NodePermissions(read=True, write_metadata=True, delete=True)
    return [
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="",
            canonical_path="/",
            stable_id="root",
            parent_stable_id=None,
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="a",
            canonical_path="/a",
            stable_id="node:/a",
            parent_stable_id="root",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="b",
            canonical_path="/a/b",
            stable_id="node:/a/b",
            parent_stable_id="node:/a",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.IMAGE,
            display_name="leaf",
            canonical_path="/a/b/leaf",
            stable_id="img:/a/b/leaf",
            parent_stable_id="node:/a/b",
            permissions=NodePermissions(read=True, write_metadata=False, delete=True),
            metadata={"type": "Image", "properties": {}},
        ),
    ]


def test_properties_size_hint_matches_leaf_payload_bytes() -> None:
    """EE st_size hint must include trailing newline so cat/mount reads match payload."""
    leaf = _seed_nodes()[3]
    md = dict(leaf.metadata or {})
    hint = leaf_properties_payload_size_hint(md, leaf.stable_id)
    payload = PyFuseService._leaf_properties_payload(leaf)
    assert hint == len(payload)
    assert payload.endswith(b"\n")


def test_fuse_sidecar_paths_rejected() -> None:
    assert is_fuse_client_sidecar_path(
        "/ee/users/u/legacy-assets/coll/._dgm1_32_424_6002_1_sh_2005"
    )
    assert not is_fuse_client_sidecar_path("/ee/users/u/legacy-assets/coll/dgm1_real")
    assert not is_fuse_client_sidecar_path("/ee/users/x/.meta.json")


def test_normalize_path_collapses_dotdot() -> None:
    assert normalize_path("/a/./b/../c") == "/a/c"
    try:
        normalize_path("/../../x")
        raise AssertionError("expected FuseError for escaping root")
    except FuseError as exc:
        assert exc.code == 22


def test_readdir_meta_only_on_first_page() -> None:
    svc = PyFuseService(InMemoryBackend(_seed_nodes()), page_size=1)
    page0 = svc.readdir("/a", offset=0, limit=1)
    page1 = svc.readdir("/a", offset=1, limit=1)
    assert ".meta.json" in page0
    assert ".meta.json" not in page1


def test_inmemory_rename_directory_moves_descendants() -> None:
    backend = InMemoryBackend(_seed_nodes())
    backend.rename("/a", "/renamed")
    moved = backend.get_node("/renamed/b/leaf")
    assert moved.display_name == "leaf"
    try:
        backend.get_node("/a/b/leaf")
        raise AssertionError("old subtree should no longer exist")
    except FuseError as exc:
        assert exc.code == 2


def test_inmemory_collision_uses_eexist() -> None:
    nodes = _seed_nodes()
    nodes.append(
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="exists",
            canonical_path="/exists",
            stable_id="node:/exists",
            parent_stable_id="root",
            permissions=NodePermissions(read=True, write_metadata=True, delete=True),
        )
    )
    backend = InMemoryBackend(nodes)
    try:
        backend.mkdir("/", "exists")
        raise AssertionError("expected collision")
    except FuseError as exc:
        assert exc.code == 17

