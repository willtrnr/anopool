"""Generic object pool"""

from __future__ import annotations

__all__ = [
    "Manager",
    "Pool",
]

import contextlib
import dataclasses
import logging
import queue
import threading
from abc import ABCMeta
from typing import Generator, Generic, TypeVar

from ._common import DEFAULT_SIZE
from .exceptions import PoolClosedError

logger = logging.getLogger(__name__)


_T = TypeVar("_T")


class Manager(Generic[_T], metaclass=ABCMeta):
    """An pool object manager.

    Manages the lifecycle of pool objects.
    """

    def create(self) -> _T:
        """Create a new pool object."""

    def recycle(self, __obj: _T) -> None:
        """Check liveness and reset released objects.

        If the object is no longer valid, this method should raise an exception to
        signal it and prevent its return to the pool. A slot will be open to allow its
        replacement.

        Args:
            obj: The returned pool object.

        Raises:
            Exception: When the object is no longer valid.
        """

    def discard(self, __obj: _T) -> None:
        """Perform cleanup of discarded objects.

        This method is called for discarding both invalid objects that failed the
        recycling and live objects on pool closure. Liveness should not be assumed and
        this method should ideally not raise any exception unless there's a failure
        that will lead to a resource leak.

        Args:
            obj: The object to be discarded.
        """


# pylint: disable=missing-class-docstring
@dataclasses.dataclass
class PoolState(Generic[_T]):
    is_open: threading.Event
    count: threading.Semaphore
    lock: threading.Condition
    idle: queue.SimpleQueue[_T]


class Pool(Generic[_T]):
    """An object pool.

    Args:
        manager: The object manager to use.
        maxsize: Optional; The maximum number of concurrent objects available.
    """

    _manager: Manager[_T]
    _max_size: int

    _state: PoolState[_T]

    def __init__(
        self,
        manager: Manager[_T],
        max_size: int = DEFAULT_SIZE,
    ) -> None:
        if max_size <= 0:
            raise ValueError("max_size must be at least 1")
        self._manager = manager
        self._max_size = max_size
        self._init_state()

    def __enter__(self: _T_Pool) -> _T_Pool:
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, exc_tb) -> None:
        del exc_type, exc_value, exc_tb
        self.close()

    def _init_state(self) -> None:
        self._state = PoolState(
            is_open=threading.Event(),
            count=threading.BoundedSemaphore(self._max_size),
            lock=threading.Condition(lock=threading.Lock()),
            idle=queue.SimpleQueue(),
        )

    def is_open(self) -> bool:
        """Check if the pool is open.

        Returns:
            bool: Whether the pool is open.
        """
        return self._state.is_open.is_set()

    def open(self) -> None:
        """Initialize the pool."""
        self._state.is_open.set()

    def close(self) -> None:
        """Close the pool and discard its objects."""
        state = self._state

        if not state.is_open.is_set():
            return

        self._init_state()

        state.is_open.clear()
        while True:
            try:
                self._manager.discard(state.idle.get_nowait())
            except queue.Empty:
                break
            except Exception:  # pylint: disable=broad-except
                logger.warning("Discard error, possible resource leak", exc_info=True)
        with state.lock:
            state.lock.notify_all()

    @contextlib.contextmanager
    def acquire(self) -> Generator[_T, None, None]:
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
                    obj = self._manager.create()
                    logger.debug("Created new object: %s", obj)
                    break
                except:
                    state.count.release()
                    raise

            # Wait until an object is available or we can allocate more
            with state.lock:
                logger.debug("Waiting for free object or slot")
                state.lock.wait()

        try:
            yield obj
        finally:
            try:
                if not state.is_open.is_set():
                    raise PoolClosedError()

                self._manager.recycle(obj)
                logger.debug("Object succeeded recycle: %s", obj)

                if not state.is_open.is_set():
                    raise PoolClosedError()

                state.idle.put(obj)
                logger.debug("Object returned to pool: %s", obj)
            except Exception:  # pylint: disable=broad-except
                logger.debug("Recycle failed discarding: %s", obj, exc_info=True)
                try:
                    self._manager.discard(obj)
                except Exception:  # pylint: disable=broad-except
                    logger.warning(
                        "Discard error, possible resource leak", exc_info=True
                    )
                state.count.release()
            finally:
                with state.lock:
                    state.lock.notify()


_T_Pool = TypeVar("_T_Pool", bound=Pool)
