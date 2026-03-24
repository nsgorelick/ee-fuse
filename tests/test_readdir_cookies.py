from __future__ import annotations

from pyfuse.paths import META_FILENAME
from pyfuse.service import (
    PyFuseService,
    _readdir_child_cookie,
    _readdir_meta_cookie,
    _readdir_next_child_index_after,
)


def test_readdir_cookies_first_page_then_meta_then_tail() -> None:
    page0_len = 256
    assert _readdir_child_cookie(0, page0_len) == 3
    assert _readdir_child_cookie(255, page0_len) == 258
    assert _readdir_meta_cookie(page0_len) == 259
    assert _readdir_child_cookie(256, page0_len) == 260


def test_readdir_next_index_after_meta() -> None:
    page0_len = 256
    meta_ck = 259
    assert _readdir_meta_cookie(page0_len) == meta_ck
    assert _readdir_next_child_index_after(258, page0_len) == 256
    assert _readdir_next_child_index_after(meta_ck, page0_len) == 256
    assert _readdir_next_child_index_after(260, page0_len) == 257


def test_iter_readdir_stream_matches_readdir_pages() -> None:
    from pyfuse.backend import InMemoryBackend
    from pyfuse.models import Node, NodePermissions, NodeType

    writable = NodePermissions(read=True, write_metadata=True, delete=True)
    fs_root = Node(
        node_type=NodeType.DIRECTORY,
        display_name="",
        canonical_path="/",
        stable_id="root",
        parent_stable_id=None,
        permissions=writable,
    )
    children = [
        Node(
            node_type=NodeType.IMAGE,
            display_name=f"c{i}",
            canonical_path=f"/p/c{i}",
            stable_id=f"i{i}",
            parent_stable_id="p",
            permissions=writable,
            metadata={"type": "Image", "properties": {}},
        )
        for i in range(5)
    ]
    root = Node(
        node_type=NodeType.DIRECTORY,
        display_name="p",
        canonical_path="/p",
        stable_id="p",
        parent_stable_id="root",
        permissions=writable,
    )
    backend = InMemoryBackend([fs_root, root, *children])
    svc = PyFuseService(backend, page_size=2)

    def collect(after: int) -> list[tuple[str, int]]:
        return [(n, c) for n, _, c in svc.iter_readdir("/p", 0, after)]

    assert collect(0)[0] == (".", 1)
    assert collect(1)[0] == ("..", 2)
    rest0 = [x for x in collect(2) if x[0] not in (".", "..")]
    assert rest0[0] == ("c0", 3)
    assert META_FILENAME in [x[0] for x in collect(2)]
    # After cookie 3 (c0), next chunk starts at c1
    names_after_3 = [n for n, _ in collect(3)]
    assert names_after_3[0] == "c1"
