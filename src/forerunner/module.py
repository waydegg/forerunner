from __future__ import annotations

from functools import partial
from typing import Callable, List, Literal

from ipdb import set_trace

from .job import Cron


class Module:
    def __init__(
        self,
        *,
        name: str,
        modules: List[Module] = [],
        exception_callbacks: List[Callable] = [],
    ):
        self.name = name
        self.modules = modules
        self.exception_callbacks = exception_callbacks

        self.job_partials: List[partial[Cron]] = []

    def cron(
        self,
        expr: str,
        *,
        n_workers: int = 1,
        n_retries: int = 0,
        execution: Literal["sync", "async", "thread", "process"] = "async",
        eager: bool = False,
        strategy: Literal["burst", "overlap"] = "burst",
        exception_callbacks: List[Callable] = [],
    ):
        def _cron_wrapper(func: Callable):
            job_partial = partial(
                Cron,
                func=func,
                app_name=self.name,
                job_name=func.__name__,
                exception_callbacks=exception_callbacks,
                expr=expr,
                n_workers=n_workers,
                n_retries=n_retries,
                execution=execution,
                eager=eager,
                strategy=strategy,
            )
            self.job_partials.append(job_partial)
            return func

        return _cron_wrapper

    def timer(self):
        raise NotImplementedError

    def sub(self):
        raise NotImplementedError
