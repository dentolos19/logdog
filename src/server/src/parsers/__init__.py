__all__ = [
    "create_process",
    "orchestrate_files",
    "register_pipelines",
    "run_parse_job",
]


def create_process(*args, **kwargs):
    from parsers.orchestrator import create_process as _create_process

    return _create_process(*args, **kwargs)


def orchestrate_files(*args, **kwargs):
    from parsers.orchestrator import orchestrate_files as _orchestrate_files

    return _orchestrate_files(*args, **kwargs)


def register_pipelines(*args, **kwargs):
    from parsers.orchestrator import register_pipelines as _register_pipelines

    return _register_pipelines(*args, **kwargs)


def run_parse_job(*args, **kwargs):
    from parsers.orchestrator import run_parse_job as _run_parse_job

    return _run_parse_job(*args, **kwargs)
