"""Regression: AppleDouble ``._*`` getattr must not scan EE (previously triggered full listAssets)."""

from __future__ import annotations

from pyfuse.ee_backend import EarthEngineBackend
from pyfuse.errors import FuseError


def test_get_node_on_appledouble_never_hits_earth_engine() -> None:
    class _ExplodingData:
        def listAssets(self, params):  # noqa: N802
            raise AssertionError(
                f"listAssets must not run for AppleDouble paths, got {params!r}"
            )

        def getAsset(self, asset_id: str):  # noqa: N802
            raise AssertionError(
                f"getAsset must not run for AppleDouble paths, got {asset_id!r}"
            )

    backend = EarthEngineBackend.__new__(EarthEngineBackend)
    backend.ee = type("E", (), {"data": _ExplodingData()})()

    path = (
        "/ee/users/ngorelick/legacy-assets/DTM/DE/SH/"
        "._dgm1_32_424_6002_1_sh_2005"
    )
    try:
        backend.get_node(path)
    except FuseError as exc:
        assert exc.code == 2
    else:
        raise AssertionError("expected ENOENT for ._ sidecar path")
