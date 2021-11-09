"""Common private utilities"""

from __future__ import annotations

__all__ = [
    "DEFAULT_SIZE",
]

import os

DEFAULT_SIZE = min(len(os.sched_getaffinity(0)) * 4, 24) or 4
