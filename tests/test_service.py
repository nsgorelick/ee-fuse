from __future__ import annotations

from pyfuse.backend import InMemoryBackend
from pyfuse.models import Node, NodePermissions, NodeType
from pyfuse.service import PyFuseService


def _seed_nodes() -> list[Node]:
    writable = NodePermissions(read=True, write_metadata=True, delete=True)
    readonly = NodePermissions(read=True, write_metadata=False, delete=False)
    return [
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="ee",
            canonical_path="/",
            stable_id="root",
            parent_stable_id=None,
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="projects",
            canonical_path="/ee",
            stable_id="node:/ee",
            parent_stable_id="root",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="demo",
            canonical_path="/ee/projects",
            stable_id="node:/ee/projects",
            parent_stable_id="node:/ee",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="assets",
            canonical_path="/ee/projects/demo/assets",
            stable_id="node:/ee/projects/demo",
            parent_stable_id="node:/ee/projects",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.IMAGE,
            display_name="tile_001",
            canonical_path="/ee/projects/demo/assets/tile_001",
            stable_id="image:tile_001",
            parent_stable_id="node:/ee/projects/demo",
            permissions=readonly,
            metadata={
                "type": "Image",
                "properties": {"system:index": "tile_001", "foo": "bar"},
            },
        ),
        Node(
            node_type=NodeType.IMAGE_COLLECTION,
            display_name="monthly_s2",
            canonical_path="/ee/projects/demo/assets/monthly_s2",
            stable_id="collection:monthly_s2",
            parent_stable_id="node:/ee/projects/demo",
            permissions=readonly,
            metadata={"type": "ImageCollection"},
        ),
        Node(
            node_type=NodeType.VIRTUAL_MEMBER,
            display_name="2025_02",
            canonical_path="/ee/projects/demo/assets/monthly_s2/2025_02",
            stable_id="member:2025_02",
            parent_stable_id="collection:monthly_s2",
            permissions=readonly,
            metadata={"type": "Image", "properties": {"system:index": "2025_02"}},
        ),
    ]


def test_readdir_contains_meta_sidecar() -> None:
    service = PyFuseService(InMemoryBackend(_seed_nodes()))
    listing = service.readdir("/ee/projects/demo/assets/monthly_s2")
    assert ".meta.json" in listing
    assert "2025_02" in listing


def test_meta_read_produces_json_payload() -> None:
    service = PyFuseService(InMemoryBackend(_seed_nodes()))
    data = service.read("/ee/projects/demo/assets/monthly_s2/.meta.json", size=4096, offset=0)
    assert b'"node_type": "image_collection"' in data
    assert b'"display_name": "monthly_s2"' in data


def test_cat_leaf_reads_properties_json() -> None:
    service = PyFuseService(InMemoryBackend(_seed_nodes()))
    st = service.getattr("/ee/projects/demo/assets/tile_001")
    assert st["st_size"] > 0
    data = service.read("/ee/projects/demo/assets/tile_001", size=st["st_size"], offset=0)
    assert b'"properties"' in data
    assert b'"foo"' in data
    assert b'"asset_id"' in data


def test_mkdir_and_rename_roundtrip() -> None:
    service = PyFuseService(InMemoryBackend(_seed_nodes()))
    service.mkdir("/ee/projects/demo/assets/new_folder")
    listing = service.readdir("/ee/projects/demo/assets")
    assert "new_folder" in listing

    service.rename(
        "/ee/projects/demo/assets/new_folder",
        "/ee/projects/demo/assets/renamed_folder",
    )
    listing = service.readdir("/ee/projects/demo/assets")
    assert "renamed_folder" in listing
