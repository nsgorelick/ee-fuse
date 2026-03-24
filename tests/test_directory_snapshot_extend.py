from __future__ import annotations

from pyfuse.cache import TtlCache
from pyfuse.ee_backend import EarthEngineBackend
from pyfuse.models import Node, NodePermissions, NodeTimestamps, NodeType


class _ListAssetsRecorder:
    def __init__(self) -> None:
        self.params_log: list[dict] = []

    def listAssets(self, params):  # noqa: N802
        self.params_log.append(dict(params))
        parent = params.get("parent")
        token = params.get("pageToken")
        if parent != "users/u/dir":
            return {"assets": []}
        if token is None:
            return {
                "assets": [{"name": f"users/u/dir/f{i:03d}", "type": "IMAGE"} for i in range(256)],
                "nextPageToken": "p2",
            }
        if token == "p2":
            return {
                "assets": [{"name": f"users/u/dir/f{i:03d}", "type": "IMAGE"} for i in range(256, 400)],
            }
        return {"assets": []}


class _FakeEe:
    def __init__(self, data: _ListAssetsRecorder) -> None:
        self.data = data


def _minimal_backend(recorder: _ListAssetsRecorder) -> EarthEngineBackend:
    b = EarthEngineBackend.__new__(EarthEngineBackend)
    b.ee = _FakeEe(recorder)
    b.project_id = "p"
    b.legacy_user = "u"
    b._node_cache = TtlCache(60.0)
    b._member_node_cache = TtlCache(60.0)
    b._directory_listing_cache = TtlCache(60.0)
    b._directory_listing_versions = {}
    b._directory_sorted_children = {}
    b._directory_member_by_name = {}
    b._directory_version_probe_cache = TtlCache(60.0)
    b._catalog_root_cache = TtlCache(60.0)
    b._root_permissions = NodePermissions(read=True, write_metadata=True, write_content=False, delete=True)
    return b


def test_fetch_directory_snapshot_drains_all_pages_in_one_call() -> None:
    """EE pagination is exhausted in one fetch; FUSE offset/limit only slices the snapshot."""
    recorder = _ListAssetsRecorder()
    backend = _minimal_backend(recorder)
    parent = Node(
        node_type=NodeType.DIRECTORY,
        display_name="dir",
        canonical_path="/ee/users/u/legacy-assets/dir",
        stable_id="users/u/dir",
        parent_stable_id="p",
        permissions=NodePermissions(read=True, write_metadata=True, delete=True),
        timestamps=NodeTimestamps.now(),
    )
    backend.get_node = lambda path: parent  # noqa: ARG005
    backend._probe_directory_version = lambda parent_path, asset_id, is_catalog: None  # noqa: ARG005

    backend._fetch_directory_snapshot(
        parent_path="/ee/users/u/legacy-assets/dir",
        asset_id="users/u/dir",
        is_catalog=False,
    )
    assert len(recorder.params_log) == 2
    assert recorder.params_log[0].get("pageToken") is None
    assert recorder.params_log[1].get("pageToken") == "p2"
    snap = backend._directory_listing_cache.get("/ee/users/u/legacy-assets/dir")
    assert snap is not None and len(snap) == 400
    ordered = backend._directory_sorted_children.get("/ee/users/u/legacy-assets/dir")
    assert ordered is not None and len(ordered) == 400
