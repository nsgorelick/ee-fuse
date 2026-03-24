from __future__ import annotations

from .errors import enoent, einval

META_FILENAME = ".meta.json"


def normalize_path(path: str) -> str:
    if not path:
        return "/"
    parts: list[str] = []
    for part in path.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            if not parts:
                raise einval("path escapes root")
            parts.pop()
            continue
        parts.append(part)
    normalized = "/" + "/".join(parts)
    if path.endswith("/") and normalized != "/":
        return normalized
    return normalized


def split_parent(path: str) -> tuple[str, str]:
    path = normalize_path(path)
    if path == "/":
        raise enoent("root has no parent")
    parent, _, leaf = path.rpartition("/")
    if not parent:
        parent = "/"
    return parent, leaf


def is_meta_path(path: str) -> bool:
    path = normalize_path(path)
    return path.endswith("/" + META_FILENAME)


def owning_node_path(meta_path: str) -> str:
    path = normalize_path(meta_path)
    if not is_meta_path(path):
        raise enoent("path is not a metadata sidecar")
    owner = path[: -len("/" + META_FILENAME)]
    return owner if owner else "/"


def is_fuse_client_sidecar_path(path: str) -> bool:
    """
    Paths the macOS client probes that cannot exist on Earth Engine.

    AppleDouble files use the ``._<name>`` convention; treating them like real
    assets triggers a full parent ``listAssets`` scan to resolve a bogus leaf.
    """
    if is_meta_path(path):
        return False
    path = normalize_path(path)
    if path == "/":
        return False
    leaf = path.rstrip("/").split("/")[-1]
    if leaf.startswith("._"):
        return True
    return leaf in {".DS_Store", ".localized"}
