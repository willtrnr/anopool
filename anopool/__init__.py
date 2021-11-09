"""Generic sync and async object pool implementations."""

from __future__ import annotations

__all__ = [
    "AsyncManager",
    "AsyncPool",
    "Manager",
    "Pool",
    "PoolClosedError",
    "PoolError",
]

try:
    from importlib.metadata import version

    __version__ = version(__name__)
except ImportError:
    __version__ = "0.0.0-dev"

from .async_pool import AsyncManager as AsyncManager, AsyncPool as AsyncPool
from .exceptions import PoolClosedError as PoolClosedError, PoolError as PoolError
from .pool import Manager as Manager, Pool as Pool
