import asyncio
import inspect
import sys
import traceback
from contextlib import AsyncExitStack
from functools import cache
from typing import Callable, Dict, List, Literal, cast

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
        pub: asyncio.Queue | None = None,
    ):
        self.func = func
        self.job_name = job_name
        self.app_name = app_name
        self.exception_callbacks = exception_callbacks
        self.n_workers = n_workers
        self.n_retries = n_retries
        self.execution = execution
        self.pub = pub

        self.logger = logger.bind(app=f"{self.app_name}.{self.job_name}")

        self._task: asyncio.Task | None = None
        self._worker_tasks: List[asyncio.Task] = []
        self._exception_callback_tasks: List[asyncio.Task] = []
        self._stop_task: asyncio.Task | None = None

    @property
    def is_stopped(self):
        return (
            self._task is None
            and self._stop_task is None
            and len(self._worker_tasks) == 0
            and len(self._exception_callback_tasks) == 0
        )

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

    def _create_worker_task(self, *args, **kwargs):
        def callback(task: asyncio.Task):
            self._worker_tasks.remove(task)

        async def run_func():
            try:
                # Enter context manager stacks (for any dependencies)
                async with AsyncExitStack() as stack:
                    # Get dependency results
                    try:
                        dependency_results = await resolve_dependencies(
                            dependencies=self._get_dependency_kwargs(), stack=stack
                        )
                    except Exception as e:
                        self.logger.error("Exception raised by dependency")
                        raise e

                    # Run the job function
                    res = await self.func(*args, **kwargs, **dependency_results)

                    # Publish result to any queues
                    if self.pub:
                        logger.debug("publishing to queue")
                        await self.pub.put(res)

            except asyncio.CancelledError as e:
                pass
            except Exception as e:
                exc_info = sys.exc_info()
                traceback_str = "".join(traceback.format_exception(*exc_info))
                self.logger.error(
                    "Exception rasied by wrapped func", traceback=traceback_str
                )

                # TODO: remove exception_callbacks on finish
                # for cb in self.exception_callbacks:
                #     if inspect.iscoroutinefunction(cb):
                #         cb_task = asyncio.create_task(cb(self, e))
                #         self._exception_callback_tasks.append(cb_task)
                #     else:
                #         cb(self, e)

                # TODO: implement retrying logic and don't shutdown entire Job on 1
                # unhandled Exception
                self.stop()

        self.logger.debug("Running func...")
        # NOTE: why does this need to be a future and not a task?
        worker_fut = asyncio.ensure_future(run_func())
        worker_fut.add_done_callback(callback)
        asyncio.shield(worker_fut)
        self._worker_tasks.append(worker_fut)

        return worker_fut

    async def _main(self):
        raise NotImplementedError

    def start(self):
        if self._task:
            self.logger.warning("Job already started")

        def callback(_):
            self._task = None

        self._task = asyncio.create_task(self._main())
        self._task.add_done_callback(callback)

    def stop(self):
        if self.is_stopped:
            self.logger.warning("Job already stopped")
            return

        def callback(_):
            self._stop_task = None

        async def stop_():
            # Wait for main task to cancel
            cast(asyncio.Task, self._task).cancel()
            while self._task is not None:
                await asyncio.sleep(0.1)

            # Wait for worker tasks and exception callback tasks to finish
            self.logger.debug("Waiting for worker and exception callback tasks...")
            while True:
                if len(self._worker_tasks + self._exception_callback_tasks) == 0:
                    break
                await asyncio.sleep(0.1)

        self._stop_task = asyncio.create_task(stop_())
        self._stop_task.add_done_callback(callback)

    def cancel(self):
        if self._stop_task:
            self._stop_task.cancel()
            self._stop_task = None
        if self._task:
            self._task.cancel()
            self._task = None
        for task in self._worker_tasks + self._exception_callback_tasks:
            task.cancel()
