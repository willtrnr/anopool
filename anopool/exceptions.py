"""Pool exceptions."""

from __future__ import annotations

__all__ = [
    "PoolError",
    "PoolClosedError",
]


class PoolError(Exception):
    """Geneal pool exception."""


class PoolClosedError(PoolError):
    """Exception raised when an operation is done on a closed pool."""

    def __init__(self) -> None:
        super().__init__("Pool is closed")
