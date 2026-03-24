from __future__ import annotations

from pyfuse.ee_backend import EarthEngineBackend
from pyfuse.errors import FuseError


class _FakeListAssetsData:
    def listAssets(self, params):  # noqa: N802
        parent = params.get("parent")
        token = params.get("pageToken")
        if parent == "users/ngorelick/DTM" and not token:
            return {
                "assets": [
                    {"name": "projects/x/users/ngorelick/DTM/AA", "type": "FOLDER"},
                    {
                        "name": "projects/x/users/ngorelick/DTM/DE",
                        "type": "FOLDER",
                        "updateTime": "2020-01-01T00:00:00Z",
                    },
                ],
                "nextPageToken": "p2",
            }
        if parent == "users/ngorelick/DTM" and token == "p2":
            return {"assets": [{"name": "projects/x/users/ngorelick/DTM/MISS", "type": "FOLDER"}]}
        return {"assets": []}


class _FakeEe:
    def __init__(self) -> None:
        self.data = _FakeListAssetsData()


def test_get_asset_record_via_parent_listing_paginates() -> None:
    backend = EarthEngineBackend.__new__(EarthEngineBackend)
    backend.ee = _FakeEe()
    row = backend._get_asset_record_via_parent_listing("users/ngorelick/DTM/DE")
    assert row.get("updateTime") is not None


def test_get_asset_record_via_parent_listing_missing_raises() -> None:
    backend = EarthEngineBackend.__new__(EarthEngineBackend)
    backend.ee = _FakeEe()
    try:
        backend._get_asset_record_via_parent_listing("users/ngorelick/DTM/ZZ")
        raise AssertionError("expected FuseError")
    except FuseError as exc:
        assert exc.code == 2
