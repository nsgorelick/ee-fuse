from __future__ import annotations

from .errors import enoent

META_FILENAME = ".meta.json"


def normalize_path(path: str) -> str:
    if not path:
        return "/"
    parts = [p for p in path.split("/") if p not in {"", "."}]
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
