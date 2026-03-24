# pyfuse (first pass)

This repository contains a first-pass Python implementation of the design in
`EE_FUSE_DESIGN_FIRST_PASS.md`.

It focuses on filesystem-like semantics over an abstract Earth Engine backend:

- deterministic path resolution
- node metadata model
- directory listing with TTL cache
- collection-as-directory behavior
- read-only `.meta.json` virtual file generation
- read-only JSON **property view** on leaf images/tables (`cat`-friendly)
- namespace mutations (`mkdir`, `rename`, `unlink`, `rmdir`) through backend hooks

This is a service-layer implementation meant to back a FUSE adapter in a later
iteration.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

## Run the CLI

The package includes a runnable CLI with a demo in-memory backend:

```bash
python -m pyfuse ls /ee/projects/demo/assets/monthly_s2
python -m pyfuse stat /ee/projects/demo/assets/monthly_s2
python -m pyfuse meta /ee/projects/demo/assets/monthly_s2
python -m pyfuse cat /ee/projects/demo/assets/tile_001
```

On a live mount, `cat` on a leaf image/table path prints the same JSON property view.

You can also install the command entrypoint:

```bash
pip install -e .
pyfuse ls /ee/projects/demo/assets/monthly_s2
```

## Use Real Earth Engine Assets

Authenticate once (if needed):

```bash
earthengine authenticate
```

Then run commands against your live assets:

```bash
export EE_PROJECT="your-project-id"
python -m pyfuse ls --backend ee --project "$EE_PROJECT" /ee/projects/"$EE_PROJECT"/assets
python -m pyfuse ls --backend ee --project "$EE_PROJECT" /ee/catalog
python -m pyfuse meta --backend ee --project "$EE_PROJECT" /ee/projects/"$EE_PROJECT"/assets
```

You can omit `--project` if `EE_PROJECT` is set.

If your environment needs explicit OAuth before initialization, use:

```bash
python -m pyfuse ls --backend ee --project "$EE_PROJECT" --authenticate --auth-mode localhost /ee/projects/"$EE_PROJECT"/assets
```

### Legacy user assets

Enable legacy namespace view with a user:

```bash
python -m pyfuse ls --backend ee --project "$EE_PROJECT" --legacy-user YOUR_USER /ee/users/YOUR_USER/legacy-assets
```

You can also set `EE_LEGACY_USER` and omit the flag.

If omitted, `pyfuse` attempts to auto-detect a legacy user from accessible
Earth Engine legacy roots.

## Optional FUSE mount

`mount` is available when `fusepy` is installed:

```bash
pip install -e ".[fuse]"
pyfuse mount /tmp/ee-mount --foreground
```

On macOS you may need a FUSE provider (for example macFUSE) available to the
system.

### Using fuse-t (kext-less) on macOS

If `fuse-t` is installed, use its library explicitly:

```bash
pyfuse mount --backend ee --project "$EE_PROJECT" --authenticate --auth-mode localhost /tmp/ee-mount --foreground --fuse-library /usr/local/lib/libfuse-t.dylib
```
