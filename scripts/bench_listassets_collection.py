from __future__ import annotations

import json
import os
import sys
import time


def _require_ee():
    try:
        import ee  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("Install earthengine-api: pip install earthengine-api") from exc
    project = os.environ.get("EE_PROJECT", "ngorelick")
    ee.Initialize(project=project)
    return ee


def _fetch_all(
    ee, parent: str, *, view: str, page_size: int
) -> tuple[int, int, list[dict], float]:
    """(pages, num_images, image_rows, sum_seconds_inside_listAssets)."""
    token: str | None = None
    pages = 0
    rows: list[dict] = []
    t_http = 0.0
    while True:
        pages += 1
        params: dict = {"parent": parent, "pageSize": page_size, "view": view}
        if token:
            params["pageToken"] = token
        t0 = time.perf_counter()
        resp = ee.data.listAssets(params)
        t_http += time.perf_counter() - t0
        assets = resp.get("assets", []) if isinstance(resp, dict) else []
        for a in assets:
            if (a.get("type") or "").upper() == "IMAGE":
                rows.append(a)
        token = resp.get("nextPageToken") if isinstance(resp, dict) else None
        if not token:
            break
    return pages, len(rows), rows, t_http


def _cpu_hints_like_pyfuse(assets: list[dict]) -> float:
    t0 = time.perf_counter()
    for a in assets:
        props = (a.get("properties") or {}) if isinstance(a, dict) else {}
        body = {
            "asset_id": a.get("name", ""),
            "type": a.get("type"),
            "properties": props,
        }
        for key in ("startTime", "endTime", "sizeBytes"):
            if a.get(key) is not None:
                body[key] = a[key]
        _ = len(json.dumps(body, indent=2, sort_keys=True).encode("utf-8"))
    return time.perf_counter() - t0


def main() -> None:
    parent = sys.argv[1] if len(sys.argv) > 1 else "users/ngorelick/DTM/DE/SH"
    page_size = int(os.environ.get("PAGE_SIZE", "1000"))
    ee = _require_ee()

    print(f"parent={parent!r} page_size={page_size} EE_PROJECT={os.environ.get('EE_PROJECT', 'ngorelick')!r}")
    print()

    for view in ("FULL", "BASIC"):
        wall0 = time.perf_counter()
        pages, n_img, rows, t_http = _fetch_all(ee, parent, view=view, page_size=page_size)
        wall = time.perf_counter() - wall0
        overhead = wall - t_http
        print(f"view={view}")
        print(f"  pages={pages}  images={n_img}")
        print(f"  sum_http_s={t_http:.3f}  wall_total_s={wall:.3f}  non_http_s={overhead:.3f}")
        if wall > 0:
            print(f"  http_%_of_wall={100.0 * t_http / wall:.1f}%")
        print()

    print("=== FULL fetch again: HTTP only vs local json.dumps (like size hints) ===")
    wall0 = time.perf_counter()
    pages, n_img, rows, t_http = _fetch_all(ee, parent, view="FULL", page_size=page_size)
    t_cpu = _cpu_hints_like_pyfuse(rows)
    wall = time.perf_counter() - wall0
    print(f"  pages={pages}  images={n_img}")
    print(f"  sum_http_s={t_http:.3f}  cpu_json_hints_s={t_cpu:.3f}  wall_total_s={wall:.3f}")
    rest = wall - t_http - t_cpu
    print(f"  residual_s=(wall-http-cpu)={rest:.3f}")
    if wall > 0:
        print(
            f"  http_{100 * t_http / wall:.1f}%  "
            f"cpu_hints_{100 * t_cpu / wall:.1f}%  "
            f"residual_{100 * rest / wall:.1f}%"
        )


if __name__ == "__main__":
    main()
