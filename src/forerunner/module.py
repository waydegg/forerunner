from __future__ import annotations

from functools import partial
from typing import Callable, List, Literal

from ipdb import set_trace

from .job import Cron, Sub
from .queue import BaseQueue


class Module:
    def __init__(
        self,
        name: str,
        *,
        modules: List[Module] = [],
        exception_callbacks: List[Callable] = [],
    ):
        self.name = name
        self.modules = modules
        self.exception_callbacks = exception_callbacks

        self.job_partials: List[partial[Cron] | partial[Sub]] = []

    def cron(
        self,
        expr: str,
        *,
        n_workers: int = 1,
        n_retries: int = 0,
        execution: Literal["sync", "async", "thread", "process"] = "async",
        eager: bool = False,
        exception_callbacks: List[Callable] = [],
        pub: BaseQueue | None = None,
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
                pub=pub,
            )
            self.job_partials.append(job_partial)
            return func

        return _cron_wrapper

    def timer(self):
        raise NotImplementedError

    def sub(
        self,
        queue: BaseQueue,
        *,
        n_workers: int = 1,
        n_retries: int = 0,
        execution: Literal["sync", "async", "thread", "process"] = "async",
        exception_callbacks: List[Callable] = [],
        pub: BaseQueue | None = None,
    ):
        def _sub_wrapper(func: Callable):
            job_partial = partial(
                Sub,
                func=func,
                app_name=self.name,
                job_name=func.__name__,
                exception_callbacks=exception_callbacks,
                queue=queue,
                n_workers=n_workers,
                n_retries=n_retries,
                execution=execution,
                pub=pub,
            )
            self.job_partials.append(job_partial)
            return func

        return _sub_wrapper
