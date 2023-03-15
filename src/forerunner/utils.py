import importlib
from typing import Any, Callable, List

from ipdb import set_trace

from .app import Module


def init_module_jobs(
    *,
    app_name: str,
    exception_callbacks: List[Callable],
    module: Module,
):
    """Recursively instantiate jobs for a Module

    The `app_name` and `exception_callbacks` kwargs are appended to each module's jobs
    and recursively updated to each modules children modules.
    """
    jobs = []

    parent_app_name = f"{app_name}.{module.name}"
    parent_exception_callbacks = [
        *exception_callbacks,
        *module.exception_callbacks,
    ]

    for job_partial in module.job_partials:
        job_partial.keywords["app_name"] = parent_app_name
        job_partial.keywords["exception_callbacks"].extend(parent_exception_callbacks)
        job = job_partial()
        jobs.append(job)

    for child_module in module.modules:
        child_module_jobs = init_module_jobs(
            app_name=parent_app_name,
            exception_callbacks=parent_exception_callbacks,
            module=child_module,
        )
        jobs.extend(child_module_jobs)

    return jobs


class ImportFromStringError(Exception):
    pass


def import_from_string(import_str: Any) -> Any:
    if not isinstance(import_str, str):
        return import_str

    module_str, _, attrs_str = import_str.partition(":")
    if not module_str or not attrs_str:
        message = (
            'Import string "{import_str}" must be in format "<module>:<attribute>".'
        )
        raise ImportFromStringError(message.format(import_str=import_str))

    try:
        module = importlib.import_module(module_str)
    except ImportError as exc:
        if exc.name != module_str:
            raise exc from None
        message = 'Could not import module "{module_str}".'
        raise ImportFromStringError(message.format(module_str=module_str))

    instance = module
    try:
        for attr_str in attrs_str.split("."):
            instance = getattr(instance, attr_str)
    except AttributeError:
        message = 'Attribute "{attrs_str}" not found in module "{module_str}".'
        raise ImportFromStringError(
            message.format(attrs_str=attrs_str, module_str=module_str)
        )

    return instance
