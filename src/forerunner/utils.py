from typing import Callable, List

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
