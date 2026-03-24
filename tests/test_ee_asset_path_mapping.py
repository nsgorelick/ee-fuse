from __future__ import annotations

from pyfuse.ee_backend import EarthEngineBackend, CATALOG_ROOT_ASSET


def _legacy_backend() -> EarthEngineBackend:
    b = EarthEngineBackend.__new__(EarthEngineBackend)
    b.project_id = "ngorelick"
    b.legacy_user = "ngorelick"
    return b


def test_path_from_full_legacy_resource_name() -> None:
    backend = _legacy_backend()
    p = backend._path_from_asset_id(
        "projects/earthengine-legacy/assets/users/ngorelick/DTM/AT",
        False,
    )
    assert p == "/ee/users/ngorelick/legacy-assets/DTM/AT"


def test_path_from_projects_prefix_without_assets_segment() -> None:
    """Matches EE test doubles and odd API shapes: .../users/... under projects/."""
    backend = _legacy_backend()
    p = backend._path_from_asset_id("projects/x/users/ngorelick/DTM/DE", False)
    assert p == "/ee/users/ngorelick/legacy-assets/DTM/DE"


def test_path_from_short_legacy_id_unchanged() -> None:
    backend = _legacy_backend()
    p = backend._path_from_asset_id("users/ngorelick/DTM/AT", False)
    assert p == "/ee/users/ngorelick/legacy-assets/DTM/AT"


def test_path_from_project_assets_full_resource_name() -> None:
    b = EarthEngineBackend.__new__(EarthEngineBackend)
    b.project_id = "demo"
    b.legacy_user = None
    p = b._path_from_asset_id("projects/demo/assets/folder/img", False)
    assert p == "/ee/projects/demo/assets/folder/img"


def test_path_from_catalog_resource_name() -> None:
    b = EarthEngineBackend.__new__(EarthEngineBackend)
    b.project_id = "p"
    b.legacy_user = None
    p = b._path_from_asset_id(f"{CATALOG_ROOT_ASSET}/NASA/GLDAS", True)
    assert p == "/ee/catalog/NASA/GLDAS"


def test_path_from_catalog_root() -> None:
    b = EarthEngineBackend.__new__(EarthEngineBackend)
    b.project_id = "p"
    b.legacy_user = None
    p = b._path_from_asset_id(CATALOG_ROOT_ASSET, True)
    assert p == "/ee/catalog"
