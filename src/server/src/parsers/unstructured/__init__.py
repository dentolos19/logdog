from src.parsers.unstructured import core as _core
from src.parsers.unstructured.core import *  # noqa: F401,F403
from src.parsers.unstructured.pipeline import UnstructuredPipeline


def __getattr__(name: str):
    return getattr(_core, name)


__all__ = [
    "UnstructuredPipeline",
]
