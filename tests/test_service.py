from __future__ import annotations

from pyfuse.backend import InMemoryBackend
from pyfuse.errors import FuseError
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
    data = service.read("/ee/projects/demo/assets/tile_001", size=4096, offset=0)
    assert b'"properties"' in data
    assert b'"foo"' in data
    assert b'"asset_id"' in data
    assert data.endswith(b"\n")


def test_getattr_uses_precomputed_leaf_size_hint() -> None:
    nodes = _seed_nodes()
    image = nodes[4]
    hinted = Node(
        node_type=image.node_type,
        display_name=image.display_name,
        canonical_path=image.canonical_path,
        stable_id=image.stable_id,
        parent_stable_id=image.parent_stable_id,
        permissions=image.permissions,
        timestamps=image.timestamps,
        metadata={**image.metadata, "_properties_size_hint": 123},
        etag_or_version=image.etag_or_version,
    )
    nodes[4] = hinted
    service = PyFuseService(InMemoryBackend(nodes))
    st = service.getattr("/ee/projects/demo/assets/tile_001")
    assert st["st_size"] == 123


def test_getattr_ignores_remote_size_bytes_for_json_property_view() -> None:
    nodes = _seed_nodes()
    image = nodes[4]
    sized = Node(
        node_type=image.node_type,
        display_name=image.display_name,
        canonical_path=image.canonical_path,
        stable_id=image.stable_id,
        parent_stable_id=image.parent_stable_id,
        permissions=image.permissions,
        timestamps=image.timestamps,
        metadata={**image.metadata, "sizeBytes": "987654", "_properties_size_hint": 10},
        etag_or_version=image.etag_or_version,
    )
    nodes[4] = sized
    service = PyFuseService(InMemoryBackend(nodes))
    st = service.getattr("/ee/projects/demo/assets/tile_001")
    assert st["st_size"] == 10


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


def test_getattr_includes_time_fields() -> None:
    service = PyFuseService(InMemoryBackend(_seed_nodes()))
    st = service.getattr("/ee/projects/demo/assets/tile_001")
    assert "st_mtime" in st
    assert "st_ctime" in st
    assert st["st_mtime"] >= 0


def test_write_properties_json_updates_leaf_properties() -> None:
    nodes = _seed_nodes()
    image = nodes[4]
    nodes[4] = Node(
        node_type=image.node_type,
        display_name=image.display_name,
        canonical_path=image.canonical_path,
        stable_id=image.stable_id,
        parent_stable_id=image.parent_stable_id,
        permissions=NodePermissions(read=True, write_metadata=True, delete=True),
        timestamps=image.timestamps,
        metadata=image.metadata,
        etag_or_version=image.etag_or_version,
    )
    service = PyFuseService(InMemoryBackend(nodes))
    path = "/ee/projects/demo/assets/tile_001"
    fh = 42
    service.open_for_write(path, fh=fh)
    payload = (
        b'{\n'
        b'  "asset_id": "ignored",\n'
        b'  "properties": {"foo": "baz", "new_key": 7},\n'
        b'  "type": "Image"\n'
        b'}\n'
    )
    service.truncate(path, fh=fh, length=0)
    service.write(path, fh=fh, offset=0, data=payload)
    service.release_write(path, fh=fh)
    data = service.read(path, size=4096, offset=0)
    assert b'"foo": "baz"' in data
    assert b'"new_key": 7' in data


def test_write_rejects_system_properties() -> None:
    nodes = _seed_nodes()
    image = nodes[4]
    nodes[4] = Node(
        node_type=image.node_type,
        display_name=image.display_name,
        canonical_path=image.canonical_path,
        stable_id=image.stable_id,
        parent_stable_id=image.parent_stable_id,
        permissions=NodePermissions(read=True, write_metadata=True, delete=True),
        timestamps=image.timestamps,
        metadata=image.metadata,
        etag_or_version=image.etag_or_version,
    )
    service = PyFuseService(InMemoryBackend(nodes))
    path = "/ee/projects/demo/assets/tile_001"
    fh = 43
    service.open_for_write(path, fh=fh)
    payload = b'{"properties": {"system:index": "nope"}}'
    service.truncate(path, fh=fh, length=0)
    service.write(path, fh=fh, offset=0, data=payload)
    try:
        service.release_write(path, fh=fh)
        raise AssertionError("expected FuseError")
    except FuseError as exc:
        assert exc.code == 13
