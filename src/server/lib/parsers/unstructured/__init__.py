"""Unstructured parsing package.

This package consolidates unstructured parsing helpers and the parser pipeline.
"""

from . import core as _core
from .core import *  # noqa: F401,F403
from .pipeline import UnstructuredPipeline


def __getattr__(name: str):
    """Fallback attribute access to keep helper imports stable."""
    return getattr(_core, name)


__all__ = [
    "UnstructuredPipeline",
]
