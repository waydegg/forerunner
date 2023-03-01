import asyncio
import inspect
import sys
import traceback
from contextlib import AsyncExitStack
from functools import cache
from typing import Any, Callable, Dict, List, Literal, cast

import structlog
from ipdb import set_trace

from forerunner.dependency.depends import Depends
from forerunner.dependency.utils import resolve_dependencies
from forerunner.queue.queue import BasePayload, BaseQueue

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
        pub: BaseQueue | None = None,
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

        self._worker_sem = asyncio.BoundedSemaphore(self.n_workers)

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

    def _create_worker_task(self, *, payload: BasePayload | None = None):
        def callback(task: asyncio.Task):
            self._worker_tasks.remove(task)

        async def retry_func_wrapper(*args, **kwargs):
            n_attempt = 0
            while n_attempt <= self.n_retries:
                try:
                    res = await self.func(*args, **kwargs)
                except Exception:
                    exc_info = sys.exc_info()
                    traceback_str = "".join(traceback.format_exception(*exc_info))
                    self.logger.error(
                        "Exception raised by wrapped func. Retrying...",
                        n_attempt=n_attempt,
                        traceback=traceback_str,
                    )
                    n_attempt += 1
                else:
                    return (True, res)

            return (False, None)

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

                    func_args = []
                    if payload:
                        func_args.append(payload)

                    # Run the job function
                    # TODO: handle running sync, async, thread, or process here
                    is_success, res = await retry_func_wrapper(
                        *func_args, **dependency_results
                    )

                    if not is_success:
                        logger.warning("Max retries reached", n_retries=self.n_retries)
                    else:
                        # Publish result to any queue(s)
                        if self.pub and res is not None:
                            await self.pub.push(res)

                        # MONKEY PATCH (for `Sub` jobs)
                        is_sub = hasattr(self, "queue")
                        if is_sub and payload is not None:
                            queue = cast(BaseQueue, getattr(self, "queue"))
                            await queue.ack(payload)

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
            finally:
                self._worker_sem.release()

        self.logger.debug("Running func...")

        worker_task = asyncio.create_task(run_func())
        worker_task.add_done_callback(callback)
        asyncio.shield(worker_task)
        self._worker_tasks.append(worker_task)

    async def _main(self):
        raise NotImplementedError

    def start(self):
        if self._task:
            self.logger.warning("Job already started")

        async def start_():
            try:
                await self._main()
            except asyncio.CancelledError:
                pass
            finally:
                self._task = None
                self.logger.debug("Main task stopped")

        self._task = asyncio.create_task(start_())

    def stop(self):
        if self._stop_task is not None or self.is_stopped:
            return

        async def stop_():
            try:
                assert isinstance(self._task, asyncio.Task)

                # Wait for main task to cancel
                self._task.cancel()
                while self._task is not None:
                    await asyncio.sleep(0.1)

                # Wait for worker tasks and exception callback tasks to finish
                self.logger.debug("Waiting for worker and exception callback tasks...")
                while True:
                    if len(self._worker_tasks + self._exception_callback_tasks) == 0:
                        break
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                pass
            finally:
                self._stop_task = None
                self.logger.debug("Job has stopped")

        self._stop_task = asyncio.create_task(stop_())

    def cancel(self):
        if self._stop_task:
            self._stop_task.cancel()
            self._stop_task = None
        if self._task:
            self._task.cancel()
            self._task = None
        for task in self._worker_tasks + self._exception_callback_tasks:
            task.cancel()
