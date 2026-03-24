from __future__ import annotations

from pyfuse.cache import TtlCache
from pyfuse.ee_backend import EarthEngineBackend
from pyfuse.models import Node, NodePermissions, NodeTimestamps, NodeType


class _FakeEeData:
    def __init__(self) -> None:
        self._calls = 0

    def listAssets(self, params):  # noqa: N802
        self._calls += 1
        if self._calls == 1:
            return {
                "assets": [
                    {"name": "users/u/coll/img_0", "type": "IMAGE", "properties": {}},
                    {"name": "users/u/coll/img_1", "type": "IMAGE", "properties": {}},
                ],
                "nextPageToken": "p2",
            }
        return {
            "assets": [
                {"name": "users/u/coll/img_2", "type": "IMAGE", "properties": {}},
                {"name": "users/u/coll/img_3", "type": "IMAGE", "properties": {}},
            ]
        }


class _FakeEe:
    def __init__(self) -> None:
        self.data = _FakeEeData()


def _minimal_backend() -> EarthEngineBackend:
    b = EarthEngineBackend.__new__(EarthEngineBackend)
    b.ee = _FakeEe()
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
    b._root_permissions = NodePermissions(
        read=True, write_metadata=True, write_content=False, delete=True, share=True
    )
    return b


def test_collection_members_paginates_for_offset() -> None:
    backend = _minimal_backend()
    parent = Node(
        node_type=NodeType.IMAGE_COLLECTION,
        display_name="coll",
        canonical_path="/ee/users/u/legacy-assets/coll",
        stable_id="coll",
        parent_stable_id="parent",
        permissions=NodePermissions(read=True, write_metadata=True, delete=True, share=True),
        timestamps=NodeTimestamps.now(),
    )
    backend.get_node = lambda path: parent  # noqa: ARG005
    backend._asset_id_from_path = lambda p: ("users/u/coll", False)  # noqa: ARG005
    backend._probe_directory_version = lambda *args, **kwargs: None  # noqa: ARG005

    members = backend.list_children(parent.canonical_path, offset=3, limit=1)
    assert len(members) == 1
    assert members[0].stable_id.endswith("img_3")
    assert backend.ee.data._calls == 2
