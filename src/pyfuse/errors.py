from __future__ import annotations

import errno
from dataclasses import dataclass


@dataclass(slots=True)
class FuseError(Exception):
    code: int
    message: str

    def __str__(self) -> str:
        return f"[errno={self.code}] {self.message}"


def enoent(message: str) -> FuseError:
    return FuseError(errno.ENOENT, message)


def eacces(message: str) -> FuseError:
    return FuseError(errno.EACCES, message)


def eperm(message: str) -> FuseError:
    return FuseError(errno.EPERM, message)


def enotdir(message: str) -> FuseError:
    return FuseError(errno.ENOTDIR, message)


def eisdir(message: str) -> FuseError:
    return FuseError(errno.EISDIR, message)


def eagain(message: str) -> FuseError:
    return FuseError(errno.EAGAIN, message)


def eexist(message: str) -> FuseError:
    return FuseError(errno.EEXIST, message)


def einval(message: str) -> FuseError:
    return FuseError(errno.EINVAL, message)


def enotsup(message: str) -> FuseError:
    return FuseError(errno.ENOTSUP, message)
