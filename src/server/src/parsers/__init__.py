from __future__ import annotations

from typing import Any

__all__ = [
    "orchestrate_files",
    "register_pipelines",
    "run_parse_job",
    "create_process",
    "enqueue_process",
    "mark_process_failed",
    "get_pipeline_stats",
]


def orchestrate_files(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import orchestrate_files as _orchestrate_files

    return _orchestrate_files(*args, **kwargs)


def register_pipelines(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import register_pipelines as _register_pipelines

    return _register_pipelines(*args, **kwargs)


def run_parse_job(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import run_parse_job as _run_parse_job

    return _run_parse_job(*args, **kwargs)


def create_process(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import create_process as _create_process

    return _create_process(*args, **kwargs)


def enqueue_process(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import enqueue_process as _enqueue_process

    return _enqueue_process(*args, **kwargs)


def mark_process_failed(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import mark_process_failed as _mark_process_failed

    return _mark_process_failed(*args, **kwargs)


def get_pipeline_stats(*args: Any, **kwargs: Any) -> Any:
    from parsers.orchestrator import get_pipeline_stats as _get_pipeline_stats

    return _get_pipeline_stats(*args, **kwargs)
