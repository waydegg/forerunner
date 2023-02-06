import asyncio
import inspect
import sys
import traceback
from contextlib import AsyncExitStack, asynccontextmanager
from functools import cache
from typing import Any, Callable, Dict, List, Literal, Tuple

import structlog
from ipdb import set_trace

from forerunner.dependency.depends import Depends
from forerunner.dependency.utils import resolve_dependencies

logger = structlog.get_logger()


class Job:
    def __init__(
        self,
        *,
        func: Callable,
        job_name: str,
        app_name: str,
        exception_callbacks: List[Callable] = [],
        n_workers: int,
        n_retries: int,
        execution: Literal["sync", "async", "thread", "process"],
        strategy: Literal["burst", "overlap"],
    ):
        self.func = func
        self.job_name = job_name
        self.app_name = app_name
        self.exception_callbacks = exception_callbacks
        self.n_workers = n_workers
        self.n_retries = n_retries
        self.execution = execution
        self.strategy = strategy

        self.logger = logger.bind(app=f"{self.app_name}.{self.job_name}")

        self._start_task = None
        self._stop_task = None
        self._worker_tasks = []
        self._exception_callback_tasks = []

    @property
    def is_stopped(self):
        return self._start_task is None and self._stop_task is None

    def _run_as_main(self):
        raise NotImplementedError

    def _run_as_coroutine(self):
        raise NotImplementedError

    def _run_as_thread(self):
        raise NotImplementedError

    def _run_as_process(self):
        raise NotImplementedError

    def _get_func_args(self):
        ...

    async def _get_func_kwargs(self):
        ...

    @cache
    def _get_dependency_kwargs(self) -> Dict[str, Depends]:
        dependency_kwargs = {}

        signature = inspect.signature(self.func)
        for param_name, param in signature.parameters.items():
            if type(param.default) == Depends:
                dependency_kwargs[param_name] = param.default

        return dependency_kwargs

    async def _func_wrapper(self):
        async with AsyncExitStack() as stack:
            dependency_results = await resolve_dependencies(
                dependencies=self._get_dependency_kwargs(), stack=stack
            )
            await self.func(**dependency_results)

    def _create_worker_task(self):
        def callback(task: asyncio.Task):
            try:
                task.result()
            except asyncio.CancelledError as e:
                pass  # Task cancellation should not be ignored
            except Exception as e:
                exc_info = sys.exc_info()
                traceback_str = "".join(traceback.format_exception(*exc_info))
                self.logger.error(
                    f"Exception rasied by wrapped func",
                    task_name=task.get_name(),
                    exception=e.__repr__(),
                    traceback=traceback_str,
                )

                for cb in self.exception_callbacks:
                    if inspect.iscoroutinefunction(cb):
                        cb_task = asyncio.create_task(cb(self, e))
                        self._exception_callback_tasks.append(cb_task)
                    else:
                        cb(self, e)

                # TODO: implement retrying logic and don't shutdown entire Job on 1
                # unhandled Exception
                self.stop()
            finally:
                self._worker_tasks.remove(task)

        self.logger.debug("Running func...")
        # worker_fut = asyncio.ensure_future(self.func())
        worker_fut = asyncio.ensure_future(self._func_wrapper())
        worker_fut.add_done_callback(callback)
        asyncio.shield(worker_fut)
        self._worker_tasks.append(worker_fut)

        return worker_fut

    async def _main(self):
        raise NotImplementedError

    def start(self):
        if self._start_task:
            self.logger.warning("Job already started")
            return self._start_task

        async def _start():
            try:
                await self._main()
            except asyncio.CancelledError:
                raise  # Propagate CancelledError for `.stop()` sequence
            except Exception as e:
                exc_info = sys.exc_info()
                traceback_str = "".join(traceback.format_exception(*exc_info))
                self.logger.error(
                    f"Exception rasied by main func",
                    task_name="_start",
                    exception=e.__repr__(),
                    traceback=traceback_str,
                )

        self._start_task = asyncio.create_task(_start())

        return self._start_task

    def stop(self):
        if self._stop_task:
            self.logger.warning("Job already stopping")
            return self._stop_task

        async def _stop():
            assert isinstance(self._start_task, asyncio.Task)
            # Wait for start task to cancel
            self._start_task.cancel()
            while not self._start_task.cancelled():
                await asyncio.sleep(0.1)

            # Wait for worker tasks and exception callback tasks to finish
            self.logger.debug("Waiting for worker and exception callback tasks...")
            while True:
                if len(self._worker_tasks + self._exception_callback_tasks) == 0:
                    break
                await asyncio.sleep(0.1)

            # Set start/stop tasks to `None`
            self._start_task = None
            self._stop_task = None

        self._stop_task = asyncio.create_task(_stop())

        return self._stop_task

    def cancel(self):
        if self._start_task:
            self._start_task.cancel()
            self._start_task = None
        if self._stop_task:
            self._stop_task.cancel()
            self._stop_task = None
        for task in self._worker_tasks + self._exception_callback_tasks:
            task.cancel()
