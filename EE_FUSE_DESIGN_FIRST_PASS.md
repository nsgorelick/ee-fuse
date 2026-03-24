# Earth Engine Filesystem (FUSE) Design - First Pass

## Purpose

Define a first-pass design for exposing Google Earth Engine (EE) assets through a mounted filesystem, with a strong emphasis on:

- navigating containers and assets using standard filesystem operations (`cd`, `ls`, `pwd`)
- making collections navigable without special path syntax
- providing deterministic identity, metadata, and permissions behavior
- deferring uploads and long-running ingestion workflows to a later phase

This document is intentionally scoped to read/navigation and basic asset namespace mutation (create/rename/delete where supported).

## Non-Goals (Phase 0 / 1)

- implementing raster/feature data uploads
- exposing all EE compute graph operations as file operations
- implementing full POSIX semantics (hard links, random writes, mmap guarantees)
- replacing existing Earth Engine SDK or CLI workflows

## Design Principles

1. **Use standard shell behavior first**  
   Users should rely on existing shell tools and muscle memory, not a custom command language.

2. **Filesystem metaphor, not filesystem illusion**  
   Present a practical, scriptable mapping while being explicit where EE diverges from POSIX.

3. **Deterministic naming and identity**  
   Every visible node should have stable path behavior and a stable internal identity.

4. **Fast enough by default**  
   Directory listing and stat operations should use bounded caches and pagination-aware backend calls.

5. **Make remote consistency understandable**  
   Prefer a bounded-staleness model with explicit refresh semantics.

## High-Level Architecture

1. **FUSE Adapter**
   - Implements `readdir`, `getattr`, `lookup`, `mkdir`, `rename`, `unlink` (subset), and read-only file open/read for virtual metadata files.
   - Maps kernel calls to internal node API.

2. **EE Backend Client**
   - Wraps Earth Engine API calls.
   - Normalizes backend responses into typed node records.

3. **Path Resolver and Identity Index**
   - Resolves canonical paths to internal node IDs.
   - Maintains short-lived mapping cache (path -> node identity + version stamp).

4. **Metadata/Listing Cache**
   - TTL-based caches for directory entries and node metadata.
   - Invalidation on local mutations and periodic refresh.

5. **Auth Session Manager**
   - Handles ADC/OAuth credentials and token refresh.
   - Surfaces auth failures as standard filesystem errors where possible.

## Mount Layout

Canonical mount root:

- `/ee/projects/<project-id>/assets/...` - authoritative project-scoped assets namespace (read/write subject to permission)
- `/ee/users/<user>/legacy-assets/...` - optional compatibility view for legacy user asset organization
- `/ee/catalog/...` - public datasets, read-only
- `/ee/system/tasks/...` - optional future virtual view (not in Phase 1)

Notes:

- Canonical paths should be unique and stable.
- Optional alias paths may exist later as symlinks, but canonical paths are the source of truth.

### Backend mapping (actual Earth Engine asset IDs)

The visible tree is a **view** over EE’s asset namespace. In the current prototype:

- **Project assets** — canonical path `/ee/projects/<project-id>/assets/...` maps to asset id `projects/<project-id>/assets/...` (used with `ee.data.getAsset`, `ee.data.listAssets`, etc.).
- **Public catalog** — `/ee/catalog/...` maps to `projects/earthengine-public/assets/...` (read-only in the adapter).
- **Legacy user assets** — `/ee/users/<user>/legacy-assets/...` maps to `users/<user>/...`. On the wire, `getAsset("users/<user>")` may resolve to a path under Google’s legacy bucket (e.g. `projects/earthengine-legacy/assets/users/<user>`); the FUSE path stays canonical as above.

Virtual path scaffolding (no separate EE folder): `/ee`, `/ee/projects`, `/ee/projects/<project-id>`, `/ee/projects/<project-id>/assets`, and when legacy is enabled `/ee/users`, `/ee/users/<user>`, `/ee/users/<user>/legacy-assets`.

## Data Model

Each visible path resolves to a `Node` with:

- `node_type`: `directory`, `image`, `image_collection`, `table`, `catalog_dataset`, `virtual_member`
- `display_name`: basename shown in `ls`
- `canonical_path`
- `stable_id`: backend identity token where available
- `parent_stable_id`
- `permissions`: effective permissions (`read`, `write_metadata`, `write_content`, `delete`, `share`)
- `timestamps`: created/updated (best effort)
- `metadata`: type-specific summary map
- `etag_or_version`: backend or synthesized change token

`table` is used as the canonical asset type name for vector/tabular assets; this corresponds to `FeatureCollection` data in Earth Engine.

### Node Type Semantics

- **directory**
  - true namespace container
  - supports `cd`, `ls`, `mkdir`, `rename`, `rmdir` (if empty / supported)
- **image/table**
  - leaf assets by default (not browsed as directories)
  - support `stat` and metadata file reads
- **image_collection/table**
  - browsable as directories
  - `ls` shows member entries as virtual child nodes
- **virtual_member**
  - synthetic child representing a collection item
  - may be opened for metadata reads and selected export-oriented reads in later phases

## Collections as Directories

Collections are first-class directories; no special path tokens.

Examples:

- `cd /ee/projects/p/assets/dem/monthly_s2`
- `ls` -> member list
- `ls -l` -> include summary fields (index, timestamp, geometry/band hints)

### Member Naming Strategy

Deterministic name priority:

1. `system:index`
2. configured fallback property (per collection type)
3. deterministic hash-based fallback

If collisions occur, append `~<n>` suffix (`item`, `item~1`, `item~2`).

### Large Collection Strategy

- server-backed pagination for `readdir`
- local entry window cache keyed by `(collection_id, query_key, page)`
- bounded listing by default; continue on offset as kernel requests more entries
- **Also required on real FUSE:** mitigate per-entry `getattr` from `ls -l` (see *Critical: `ls` vs `ls -l`* under Consistency and Caching).

## Metadata Exposure

Expose metadata in two ways:

1. `stat`/`ls -l` summary view
2. virtual sidecar file for detailed metadata:
   - `.meta.json` available on any node

Examples:

- `/ee/projects/p/assets/dem/tile_001/.meta.json`
- `/ee/projects/p/assets/dem/monthly_s2/2025_02/.meta.json`

Virtual files are read-only and generated on demand (with cache).

### Hypothesis: leaf files as property views (`cat` the asset)

**Idea:** For leaf assets (Image, Table, and virtual collection members), the file’s *byte stream* is not raster or vector geometry—it is a **JSON document** of Earth Engine–relevant fields, centered on **`properties`** (plus `asset_id`, EE `type`, and optional `startTime` / `endTime` / `sizeBytes`). Then:

- `cat /ee/projects/.../my_image` behaves like “show me this asset’s properties” in a shell-friendly way.
- This matches user mental models (“open the file → see what it is”) without pretending the mount serves pixels through `read()`.

**Contrast with `.meta.json`:** The sidecar remains the **full** FUSE/node envelope (permissions, canonical path, node_type, nested `metadata`, etc.). The leaf `cat` payload is intentionally **smaller and task-focused** for scripting and quick inspection.

**Bi-directional editing (future):** In principle, writing the same JSON back could map to EE updates, e.g. Python client `ee.data.setAssetProperties(assetId, properties)` or `ee.data.updateAsset(...)` with an appropriate mask. That would require:

- defining merge semantics (replace-all vs patch keys vs reserved keys like `system:*`),
- permission checks (`write_metadata`),
- conflict handling (ETag / version),
- and explicit UX (e.g. only on truncate+write or atomic replace), because POSIX “partial write” does not match EE well.

Until then, treat leaf property views as **read-only** in the mount; use a dedicated tool or documented write path when implemented.

**Prototype:** `pyfuse` implements read-only leaf property JSON for `image`, `table`, `virtual_member`, and `catalog_dataset` leaves; mount and CLI `cat` use the same bytes.

## Permissions Model

Permissions are represented as effective capability flags on each node.

- Read operations require `read`.
- Namespace mutation (`mkdir`, `rename`, `rm`) requires relevant write/delete permissions on parent and target.
- Shared folders may contain mixed-permission children; listing must not assume inheritance-only behavior.

On permission failure:

- return permission-oriented filesystem errors (`EACCES`, `EPERM`) where semantically appropriate
- include detail in daemon logs for debugging

## Consistency and Caching

Consistency target: **bounded staleness with explicit refresh**.

- Metadata TTL: short (for example 10-30 seconds, configurable)
- Directory listing TTL: short (for example 5-15 seconds, configurable)
- Immediate invalidation after successful local mutations
- Optional manual refresh path (`touch .refresh` pattern or control API) in later phase

Behavior contract:

- A single `ls` call returns an internally consistent snapshot.
- Concurrent remote changes may appear after TTL or explicit invalidation.

### Critical: `ls` vs `ls -l` and kernel behavior

On real FUSE mounts, **`ls -l` (and similar tools) issue many `getattr`/`lookup` calls**—often one per directory entry. That is unrelated to how many items `readdir` returns in one batch.

**Lesson:** A naive implementation that resolves each collection member with a separate Earth Engine API call (e.g. `getAsset` per member name) will make large image collections appear to “hang” even when `listImages` itself is fast.

**Mitigations that worked in the prototype:**

1. **Prewarm the node cache from `readdir`** — when listing a directory, register each child `Node` returned by the backend into the path→node cache so immediate follow-up `getattr` from `ls -l` hits cache instead of the network.
2. **Backend node cache** — cache results of `getAsset` (and virtual scaffolding nodes) with a short TTL so repeated parent lookups during member resolution do not amplify API calls.
3. **Synthetic `getattr` for virtual collection members** — for paths under an `image_collection`, treat `/<collection>/<member_basename>` as a `virtual_member` using listing-derived identity when possible, or a lightweight synthetic node for stat-only paths, **without** calling `listImages`/`getAsset` per member on every stat.
4. **Keep paginating `readdir`** — still use `pageSize` / `nextPageToken` (or equivalent) for very large collections; the above addresses the stat storm, not replacing pagination.

## Filesystem Operation Mapping (Phase 1)

Supported:

- `lookup/getattr`
- `readdir`
- `mkdir` (asset folders only)
- `rename`
- `unlink` / `rmdir` for supported asset types
- read-only `open/read` on virtual metadata files (`.meta.json`)
- read-only `open/read` on leaf image/table (and similar) paths as **JSON property views** (see *Leaf files as property views*)

Not supported yet:

- writing node content (including property JSON round-trip)
- random writes
- append semantics
- upload triggers

## Error Mapping

Translate EE/backend failures into predictable filesystem errors:

- Not found -> `ENOENT`
- Permission denied -> `EACCES` or `EPERM`
- Invalid path/type mismatch -> `ENOTDIR` or `EISDIR` as appropriate
- Rate-limited/transient backend -> `EAGAIN` (and log retry hints)
- Unsupported operation -> `ENOTSUP`

## Security and Auth

- No credential material exposed in mounted tree.
- Auth manager handles token refresh and project-scoped context.
- Mount process runs with least privilege and explicit project allowlist where possible.
- Audit log includes request IDs, node path, operation, latency, and backend status.

### Earth Engine Python client (implementation notes)

- **`ee.Initialize(project=...)`** selects the Cloud project used for EE; it does not by itself fix missing OAuth tokens.
- Some environments need an explicit **`ee.Authenticate()`** (or CLI `earthengine authenticate`) before `Initialize`; a credentials file may exist but still be rejected until refresh/auth completes.
- **API shape drift:** In current `earthengine-api` versions, `ee.data.listAssets` and `ee.data.listImages` take a **single parameters object** (e.g. `{"parent": "<asset_id>", "pageSize": N, "view": "BASIC"}`), not `(parent, params)` as two positional arguments. Implementations should introspect or pin versions.
- **Asset roots:** `ee.data.getAssetRoots()` returns top-level assets for the account but does not replace folder listing; use `listAssets` with the correct `parent` id.

## Implementation learnings — pyfuse prototype

Rough stack:

1. **Service layer** (`PyFuseService`) — canonical paths, TTL caches for listings and nodes, `.meta.json` generation, mutation hooks.
2. **EE backend** — maps paths ↔ asset ids, calls `ee.data.*`, builds `Node` records.
3. **FUSE adapter** — `fusepy` implementing `getattr`, `readdir`, `open`/`read` (metadata only), `mkdir`/`rename`/`unlink`/`rmdir` where allowed.

**CLI** — subcommands (`ls`, `stat`, `meta`, `mount`, …) for testing without a mount; `mount` uses the same service as FUSE.

### macOS: fuse-t (kext-less FUSE)

To avoid kernel extension approval flows where possible, **fuse-t** can provide a userspace `libfuse` compatible library. The prototype sets **`FUSE_LIBRARY_PATH`** (or equivalent) to point `fusepy` at **`libfuse-t.dylib`** (e.g. under `/usr/local/lib` after install). macFUSE may still be present on the machine; explicitly selecting fuse-t avoids accidental binding to the wrong library.

### Legacy user path and autodetection

- **Explicit:** `--legacy-user <user>` or env `EE_LEGACY_USER` exposes `/ee/users/<user>/legacy-assets`.
- **Autodetect (prototype heuristic):** If not set, try `users/<project-id>` via `getAsset`; if that fails, list `projects/earthengine-legacy/assets` and pick an entry matching `.../users/<name>` (prefer `<name> == project-id` if multiple). This is a **best-effort** default for single-user dev setups; multi-user mounts should set legacy user explicitly.

## Deferred Upload/Ingestion Story

Uploads are explicitly out of scope for first pass because they are long-running and asynchronous in EE.

Future ingestion design should likely include:

- staging area in local filesystem
- explicit submit operation to EE ingestion API
- asynchronous task objects with progress/state
- idempotency keys and resume/retry logic

Until then, mount remains read/navigation plus namespace mutation only.

## Phased Implementation Plan

### Phase 0 - Read-Only Explorer

- Mount catalog and project asset trees read-only.
- Implement `readdir/getattr/lookup`.
- Add `.meta.json` virtual files.
- Add cache metrics and debug logging.

**Prototype status:** Python `pyfuse` package implements project + catalog + legacy paths, FUSE via `fusepy` (fuse-t on macOS), EE-backed listing with the caching/stat optimizations above; metrics/logging still minimal.

### Phase 1 - Namespace Mutation

- Enable `mkdir`, `rename`, `rm/rmdir` for supported nodes.
- Add permission checks and clearer error propagation.
- Improve collection listing performance and paging.

### Phase 2 - Operational Views (Optional)

- Add task/status virtual subtree.
- Add refresh controls and mount health endpoints.
- Introduce richer metadata summaries and filters.

### Phase 3 - Ingestion (Later)

- Introduce explicit upload/ingest workflow (non-POSIX semantics).
- Track tasks as first-class objects.
- Add robust retry/resume behavior.

## Open Questions

1. What is the canonical root in environments with many projects: single-project mount or multi-project mount by default?
2. Should virtual member nodes in collections be addressable for all read operations, or metadata-only in early phases? *(Prototype: metadata-oriented synthetic members for stat; full read behavior still TBD.)*
3. How should we expose query-filtered collection views without introducing custom pseudo-directories?
4. Which metadata fields are guaranteed vs best effort across all asset types?
5. ~~Do we support a user-level legacy path at all, or force project-only canonical paths from day one?~~ **Resolved for prototype:** Yes — `/ee/users/<user>/legacy-assets/...` with optional autodetection; production policy may still require explicit configuration for shared machines.

## Success Criteria for First Pass

- Users can reliably `cd` and `ls` through folders and collections.
- Path resolution and naming are deterministic across sessions.
- `ls` on large collections is responsive through paging/caching.
- Metadata is easy to discover and script against.
- Basic namespace operations work with clear permission/error behavior.
- No upload semantics required to deliver immediate usability.
