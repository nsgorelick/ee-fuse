from __future__ import annotations

from .backend import InMemoryBackend
from .models import Node, NodePermissions, NodeType


def build_demo_backend() -> InMemoryBackend:
    writable = NodePermissions(read=True, write_metadata=True, delete=True)
    readonly = NodePermissions(read=True, write_metadata=False, delete=False)
    nodes = [
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="",
            canonical_path="/",
            stable_id="root",
            parent_stable_id=None,
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="ee",
            canonical_path="/ee",
            stable_id="node:/ee",
            parent_stable_id="root",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="projects",
            canonical_path="/ee/projects",
            stable_id="node:/ee/projects",
            parent_stable_id="node:/ee",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="demo",
            canonical_path="/ee/projects/demo",
            stable_id="node:/ee/projects/demo",
            parent_stable_id="node:/ee/projects",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.DIRECTORY,
            display_name="assets",
            canonical_path="/ee/projects/demo/assets",
            stable_id="node:/ee/projects/demo/assets",
            parent_stable_id="node:/ee/projects/demo",
            permissions=writable,
        ),
        Node(
            node_type=NodeType.IMAGE_COLLECTION,
            display_name="monthly_s2",
            canonical_path="/ee/projects/demo/assets/monthly_s2",
            stable_id="collection:monthly_s2",
            parent_stable_id="node:/ee/projects/demo/assets",
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
            metadata={"system:index": "2025_02"},
        ),
    ]
    return InMemoryBackend(nodes)
