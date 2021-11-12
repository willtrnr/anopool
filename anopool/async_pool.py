"""Generic async object pool"""

from __future__ import annotations

__all__ = [
    "AsyncManager",
    "AsyncPool",
]

import asyncio
import contextlib
import dataclasses
import logging
import queue
import threading
from abc import ABCMeta
from concurrent.futures import Executor
from typing import AsyncGenerator, Generic, Optional, TypeVar

from ._common import DEFAULT_SIZE
from .exceptions import PoolClosedError
from .pool import Manager

logger = logging.getLogger(__name__)


_T = TypeVar("_T")


class AsyncManager(Generic[_T], metaclass=ABCMeta):
    """An async pool object manager.

    Manages the lifecycle of async pool objects.
    """

    async def create(self) -> _T:
        """Create a new pool object."""

    async def recycle(self, __obj: _T) -> None:
        """Check liveness and reset released objects.

        If the object is no longer valid, this method should raise an exception to
        signal it and prevent its return to the pool. A slot will be open to allow its
        replacement.

        Args:
            obj: The returned pool object.

        Raises:
            Exception: When the object is no longer valid.
        """

    async def discard(self, __obj: _T) -> None:
        """Perform cleanup of discarded objects.

        This method is called for discarding both invalid objects that failed the
        recycling and live objects on pool closure. Liveness should not be assumed and
        this method should ideally not raise any exception unless there's a failure
        that will lead to a resource leak.

        Args:
            obj: The object to be discarded.
        """

    @staticmethod
    def from_sync(manager: Manager[_T], blocker: Executor) -> AsyncManager[_T]:
        """Adapt a blocking manager into an async manager.

        Args:
            manager: The blocking manager to adapt.
            blocker: The blocking executor on which blocking calls should run.

        Returns:
            The async-enabled manager for the wrapped manager.
        """
        return WrappedSyncManager(manager, blocker)


# pylint: disable=missing-class-docstring
@dataclasses.dataclass
class AsyncPoolState(Generic[_T]):
    is_open: asyncio.Event
    count: threading.Semaphore
    lock: asyncio.Condition
    idle: queue.SimpleQueue[_T]


class AsyncPool(Generic[_T]):
    """An async object pool.

    Args:
        manager: The async object manager to use.
        maxsize: Optional; The maximum number of concurrent objects available.
    """

    _manager: AsyncManager[_T]
    _max_size: int

    _state: AsyncPoolState[_T]

    def __init__(
        self,
        manager: AsyncManager[_T],
        max_size: Optional[int] = None,
    ) -> None:
        if max_size is None:
            max_size = DEFAULT_SIZE
        elif max_size <= 0:
            raise ValueError("max_size must be at least 1")
        self._manager = manager
        self._max_size = max_size
        self._init_state()

    def _init_state(self):
        self._state = AsyncPoolState(
            is_open=asyncio.Event(),
            count=threading.BoundedSemaphore(self._max_size),
            lock=asyncio.Condition(lock=asyncio.Lock()),
            idle=queue.SimpleQueue(),
        )

    async def __aenter__(self: _T_AsyncPool) -> _T_AsyncPool:
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_tb) -> None:
        del exc_type, exc_value, exc_tb
        await self.close()

    def is_open(self) -> bool:
        """Check if the pool is open.

        Returns:
            bool: Whether the pool is open.
        """
        return self._state.is_open.is_set()

    async def open(self) -> None:
        """Initialize the pool."""
        self._state.is_open.set()

    async def close(self) -> None:
        """Close the pool and discard its objects."""
        state = self._state

        if not state.is_open.is_set():
            return

        self._init_state()

        state.is_open.clear()
        while True:
            try:
                await self._manager.discard(state.idle.get_nowait())
            except queue.Empty:
                break
            except Exception:  # pylint: disable=broad-except
                logger.warning("Discard error, possible resource leak", exc_info=True)
        async with state.lock:
            state.lock.notify_all()

    @contextlib.asynccontextmanager
    async def acquire(self) -> AsyncGenerator[_T, None]:
        """Acquire an object from the pool.

        Yields:
            An object from the pool.
        """
        state = self._state

        while True:
            if not state.is_open.is_set():
                raise PoolClosedError()

            # Try to get an object from the pool first
            try:
                obj = state.idle.get_nowait()
                logger.debug("Checked out object from pool: %s", obj)
                break
            except queue.Empty:
                pass

            # If we can allocate more, create a new one
            # pylint: disable=consider-using-with
            if state.count.acquire(blocking=False):
                try:
                    obj = await self._manager.create()
                    logger.debug("Created new object: %s", obj)
                    break
                except:
                    state.count.release()
                    raise

            # Wait until an object is available or we can allocate more
            async with state.lock:
                logger.debug("Waiting for free object or slot")
                await state.lock.wait()

        try:
            yield obj
        finally:
            try:
                if not state.is_open.is_set():
                    raise PoolClosedError()

                await self._manager.recycle(obj)
                logger.debug("Object succeeded recycle: %s", obj)

                if not state.is_open.is_set():
                    raise PoolClosedError()

                state.idle.put(obj)
                logger.debug("Object returned to pool: %s", obj)
            except Exception:  # pylint: disable=broad-except
                logger.debug("Recycle failed discarding: %s", obj, exc_info=True)
                try:
                    await self._manager.discard(obj)
                except Exception:  # pylint: disable=broad-except
                    logger.warning(
                        "Discard error, possible resource leak", exc_info=True
                    )
                state.count.release()
            finally:
                async with state.lock:
                    state.lock.notify()


_T_AsyncPool = TypeVar("_T_AsyncPool", bound=AsyncPool)


# pylint: disable=missing-class-docstring,missing-function-docstring
class WrappedSyncManager(AsyncManager[_T]):
    _wrapped: Manager[_T]
    _blocker: Executor

    def __init__(self, manager: Manager[_T], blocker: Executor) -> None:
        self._wrapped = manager
        self._blocker = blocker

    async def create(self) -> _T:
        return await asyncio.get_event_loop().run_in_executor(
            self._blocker, self._wrapped.create
        )

    async def recycle(self, obj: _T) -> None:
        return await asyncio.get_event_loop().run_in_executor(
            self._blocker, self._wrapped.recycle, obj
        )

    async def discard(self, obj: _T) -> None:
        return await asyncio.get_event_loop().run_in_executor(
            self._blocker, self._wrapped.discard, obj
        )
