from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from typing import Any

from .demo import build_demo_backend
from .ee_backend import EarthEngineBackend
from .errors import FuseError
from .service import PyFuseService

_LOG_LEVEL_CHOICES = frozenset(
    ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"),
)


def _resolve_log_level(args: argparse.Namespace) -> str:
    """Effective log level: CLI flag > PYFUSE_LOG_LEVEL > mount=DEBUG else WARNING."""
    if args.log_level is not None:
        return str(args.log_level).upper()
    env = os.getenv("PYFUSE_LOG_LEVEL")
    if env:
        u = env.strip().upper()
        if u in _LOG_LEVEL_CHOICES:
            return u
    if args.command == "mount":
        return "DEBUG"
    return "WARNING"


def _build_service(args: argparse.Namespace) -> PyFuseService:
    if args.backend == "ee":
        if not args.project:
            raise FuseError(22, "--project is required for --backend ee")
        return PyFuseService(
            EarthEngineBackend(
                args.project,
                authenticate_first=args.authenticate,
                auth_mode=args.auth_mode,
                legacy_user=args.legacy_user,
            )
        )
    return PyFuseService(build_demo_backend())


def _print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True))


def cmd_ls(args: argparse.Namespace) -> int:
    svc = _build_service(args)
    names = svc.readdir(args.path, offset=0, limit=args.limit)
    for name in names:
        print(name)
    return 0


def cmd_stat(args: argparse.Namespace) -> int:
    svc = _build_service(args)
    st = svc.getattr(args.path)
    _print_json(st)
    return 0


def cmd_meta(args: argparse.Namespace) -> int:
    svc = _build_service(args)
    target = args.path.rstrip("/") + "/.meta.json"
    data = svc.read(target, size=1024 * 1024, offset=0)
    print(data.decode("utf-8"))
    return 0


def cmd_cat(args: argparse.Namespace) -> int:
    """Read leaf asset property JSON (same bytes as `cat` on a mounted image/table)."""
    svc = _build_service(args)
    svc.open_for_read(args.path)
    # Stream in chunks instead of sizing one big buffer from getattr.
    offset = 0
    chunk_size = 64 * 1024
    last_chunk = b""
    while True:
        chunk = svc.read(args.path, size=chunk_size, offset=offset)
        if not chunk:
            break
        sys.stdout.buffer.write(chunk)
        offset += len(chunk)
        last_chunk = chunk
        if len(chunk) < chunk_size:
            break
    if last_chunk and not last_chunk.endswith(b"\n"):
        sys.stdout.buffer.write(b"\n")
    return 0


def cmd_mkdir(args: argparse.Namespace) -> int:
    svc = _build_service(args)
    node = svc.mkdir(args.path)
    _print_json({"created": node.canonical_path, "type": node.node_type.value})
    return 0


def cmd_rename(args: argparse.Namespace) -> int:
    svc = _build_service(args)
    node = svc.rename(args.source, args.destination)
    _print_json({"renamed_to": node.canonical_path, "type": node.node_type.value})
    return 0


def cmd_rm(args: argparse.Namespace) -> int:
    svc = _build_service(args)
    if args.directory:
        svc.rmdir(args.path)
    else:
        svc.unlink(args.path)
    _print_json({"removed": args.path})
    return 0


def cmd_mount(args: argparse.Namespace) -> int:
    if args.fuse_library:
        os.environ["FUSE_LIBRARY_PATH"] = args.fuse_library
    try:
        from fuse import FUSE, FuseOSError, Operations  # type: ignore

        from .fuse_readdir import offset_aware_fuse_subclass

        FUSE = offset_aware_fuse_subclass(FUSE)
    except Exception:
        print("fusepy not installed. Install with: pip install fusepy", file=sys.stderr)
        return 2

    class _Ops(Operations):
        def __init__(self, ns: argparse.Namespace) -> None:
            self.svc = _build_service(ns)
            self._fh_seq = 1
            self._fh_paths: dict[int, str] = {}
            self._write_handles: set[int] = set()

        def getattr(self, path: str, fh: int | None = None) -> dict[str, Any]:
            try:
                return self.svc.getattr(path)
            except FuseError as exc:
                raise FuseOSError(exc.code)

        def readdir(self, path: str, fh: int) -> list[str]:
            try:
                return self.svc.readdir(path)
            except FuseError as exc:
                raise FuseOSError(exc.code)

        def iter_readdir(self, path: str, fh: int, after_off: int):
            try:
                yield from self.svc.iter_readdir(path, fh, after_off)
            except FuseError as exc:
                raise FuseOSError(exc.code)

        def open(self, path: str, flags: int) -> int:
            fh = self._fh_seq
            self._fh_seq += 1
            try:
                if flags & (os.O_WRONLY | os.O_RDWR):
                    self.svc.open_for_write(path, fh=fh)
                    self._write_handles.add(fh)
                else:
                    self.svc.open_for_read(path)
                self._fh_paths[fh] = path
            except FuseError as exc:
                raise FuseOSError(exc.code)
            return fh

        def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
            try:
                return self.svc.read(path, size=size, offset=offset)
            except FuseError as exc:
                raise FuseOSError(exc.code)

        def write(self, path: str, data: bytes, offset: int, fh: int) -> int:
            try:
                return self.svc.write(path, fh=fh, offset=offset, data=data)
            except FuseError as exc:
                raise FuseOSError(exc.code)

        def truncate(self, path: str, length: int, fh: int | None = None) -> int:
            if fh is None:
                raise FuseOSError(22)  # EINVAL
            try:
                self.svc.truncate(path, fh=fh, length=length)
            except FuseError as exc:
                raise FuseOSError(exc.code)
            return 0

        def release(self, path: str, fh: int) -> int:
            try:
                if fh in self._write_handles:
                    self.svc.release_write(path, fh=fh)
            except FuseError as exc:
                raise FuseOSError(exc.code)
            finally:
                self._write_handles.discard(fh)
                self._fh_paths.pop(fh, None)
            return 0

        def mkdir(self, path: str, mode: int) -> int:
            try:
                self.svc.mkdir(path)
            except FuseError as exc:
                raise FuseOSError(exc.code)
            return 0

        def rename(self, old: str, new: str) -> int:
            try:
                self.svc.rename(old, new)
            except FuseError as exc:
                raise FuseOSError(exc.code)
            return 0

        def unlink(self, path: str) -> int:
            try:
                self.svc.unlink(path)
            except FuseError as exc:
                raise FuseOSError(exc.code)
            return 0

        def rmdir(self, path: str) -> int:
            try:
                self.svc.rmdir(path)
            except FuseError as exc:
                raise FuseOSError(exc.code)
            return 0

    FUSE(_Ops(args), args.mountpoint, foreground=args.foreground, nothreads=True)  # type: ignore[misc]
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pyfuse", description="pyfuse first-pass CLI")
    parser.add_argument(
        "--log-level",
        default=None,
        choices=sorted(_LOG_LEVEL_CHOICES),
        metavar="LEVEL",
        help=(
            "Logging verbosity. Omit to use PYFUSE_LOG_LEVEL, else mount uses DEBUG "
            "and other subcommands use WARNING. "
            "Place before the subcommand (e.g. pyfuse --log-level INFO mount …)."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    def add_backend_args(cmd: argparse.ArgumentParser) -> None:
        cmd.add_argument(
            "--backend",
            choices=("demo", "ee"),
            default="demo",
            help="Data source backend (default: demo).",
        )
        cmd.add_argument(
            "--project",
            default=os.getenv("EE_PROJECT"),
            help="Earth Engine project id (required for --backend ee).",
        )
        cmd.add_argument(
            "--authenticate",
            action="store_true",
            help="Call ee.Authenticate() before ee.Initialize() for EE backend.",
        )
        cmd.add_argument(
            "--auth-mode",
            default=None,
            help="Optional ee.Authenticate auth_mode (e.g. localhost, gcloud, notebook).",
        )
        cmd.add_argument(
            "--legacy-user",
            default=os.getenv("EE_LEGACY_USER"),
            help="Enable legacy user namespace at /ee/users/<user>/legacy-assets.",
        )

    ls = sub.add_parser("ls", help="List directory entries")
    add_backend_args(ls)
    ls.add_argument("path")
    ls.add_argument("--limit", type=int, default=1000)
    ls.set_defaults(func=cmd_ls)

    st = sub.add_parser("stat", help="Show stat-like attributes")
    add_backend_args(st)
    st.add_argument("path")
    st.set_defaults(func=cmd_stat)

    meta = sub.add_parser("meta", help="Print .meta.json for a node")
    add_backend_args(meta)
    meta.add_argument("path")
    meta.set_defaults(func=cmd_meta)

    cat = sub.add_parser("cat", help="Print JSON property view for a leaf image/table (like cat on mount)")
    add_backend_args(cat)
    cat.add_argument("path")
    cat.set_defaults(func=cmd_cat)

    mkdir = sub.add_parser("mkdir", help="Create a directory node")
    add_backend_args(mkdir)
    mkdir.add_argument("path")
    mkdir.set_defaults(func=cmd_mkdir)

    rn = sub.add_parser("rename", help="Rename a node")
    add_backend_args(rn)
    rn.add_argument("source")
    rn.add_argument("destination")
    rn.set_defaults(func=cmd_rename)

    rm = sub.add_parser("rm", help="Delete node")
    add_backend_args(rm)
    rm.add_argument("path")
    rm.add_argument("-d", "--directory", action="store_true", help="Remove as directory")
    rm.set_defaults(func=cmd_rm)

    mount = sub.add_parser("mount", help="Mount via fusepy (optional)")
    add_backend_args(mount)
    mount.add_argument("mountpoint")
    mount.add_argument("--foreground", action="store_true")
    mount.add_argument(
        "--fuse-library",
        default="/usr/local/lib/libfuse-t.dylib",
        help="Path to libfuse implementation (default: fuse-t dylib).",
    )
    mount.set_defaults(func=cmd_mount)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    level_name = _resolve_log_level(args)
    logging.basicConfig(
        level=getattr(logging, level_name, logging.WARNING),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        return args.func(args)
    except FuseError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
