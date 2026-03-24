"""
FUSE readdir bridge: fusepy's default implementation ignores the kernel directory
offset and assigns cookie 0 for plain string entries. Large directories then
never complete (kernel retries from the start). This module provides a FUSE
subclass that forwards the offset to PyFuseService.iter_readdir.
"""

from __future__ import annotations


def offset_aware_fuse_subclass(FUSE: type) -> type:
    """Return a subclass of fuse.FUSE whose readdir honors directory offsets."""

    class OffsetAwareFUSE(FUSE):
        def readdir(self, path, buf, filler, off, fip):  # noqa: N802
            from fuse import c_stat, set_st_attrs  # type: ignore

            path_d = self._decode_optional_path(path)
            fh = fip.contents.fh
            for name, attrs, cookie in self.operations("iter_readdir", path_d, fh, off):
                st = None
                if attrs is not None:
                    st = c_stat()
                    set_st_attrs(st, attrs, use_ns=self.use_ns)
                enc = name.encode(self.encoding, errors="surrogateescape")
                if filler(buf, enc, st, cookie) != 0:
                    break
            return 0

    return OffsetAwareFUSE
