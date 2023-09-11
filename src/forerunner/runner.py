import asyncio
import traceback
from datetime import datetime
from inspect import iscoroutinefunction
from typing import Callable, Dict, List, Literal, Set, Union, cast

import structlog
from croniter import croniter
from ipdb import set_trace

logger = structlog.get_logger()


class Runner:
    def __init__(
        self,
        func: Callable,
        n_workers: int,
        n_retries: int,
    ):
        self.func = func
        self.n_workers = n_workers
        self.n_retries = n_retries

        self._main_task: Union[asyncio.Task, None] = None
        self._stop_task: Union[asyncio.Task, None] = None
        self._func_tasks: Set[asyncio.Task] = set()
        self._func_sem = asyncio.BoundedSemaphore(self.n_workers)

    @property
    def is_stopped(self):
        return (
            self._main_task is None
            and self._stop_task is None
            and len(self._func_tasks) == 0
        )

    def _create_func_task(self):
        async def run_func():
            logger.debug("Running func...")
            try:
                n_attempt = 0
                while n_attempt <= self.n_retries:
                    try:
                        if iscoroutinefunction(self.func):
                            await self.func()
                            break
                        else:
                            self.func()
                            break
                    except Exception:
                        logger.error(
                            "Exception raised by wrapped func. Retrying...",
                            n_attempt=n_attempt,
                        )
                        traceback.print_exc()
                        n_attempt += 1
                if n_attempt > self.n_retries:
                    logger.warning("Max retries reached", n_retries=self.n_retries)
            except asyncio.CancelledError as e:
                pass
            except Exception:
                logger.error(
                    "Unexpected exception raised, stopping worker.",
                    func=self.func.__name__,
                )
                traceback.print_exc()
                self.stop()

        def cleanup(task: asyncio.Task):
            self._func_tasks.remove(task)
            self._func_sem.release()

        func_task = asyncio.create_task(run_func())
        func_task.add_done_callback(cleanup)
        asyncio.shield(func_task)
        self._func_tasks.add(func_task)

    async def _main(self):
        raise NotImplementedError

    def start(self):
        if self._main_task:
            logger.warning("Job already started")
            return

        async def run_main():
            try:
                await self._main()
            except asyncio.CancelledError:
                pass

        def cleanup_main(_: asyncio.Task):
            self._main_task = None
            logger.debug("Main task stopped")

        self._main_task = asyncio.create_task(run_main())
        self._main_task.add_done_callback(cleanup_main)

    def stop(self):
        if self._stop_task is not None:
            logger.warning("Job is already stopping")
            return
        if self.is_stopped:
            logger.warning("Job is already stopped")
            return

        async def run_stop():
            try:
                assert isinstance(self._main_task, asyncio.Task)

                # Wait for main task to cancel
                self._main_task.cancel()
                while self._main_task is not None:
                    await asyncio.sleep(0.1)

                # Wait for worker tasks and exception callback tasks to finish
                logger.debug("Waiting for worker and exception callback tasks...")
                while len(self._func_tasks) > 0:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                pass

        def cleanup_stop(_: asyncio.Task):
            self._stop_task = None
            logger.debug("Job has stopped")

        self._stop_task = asyncio.create_task(run_stop())
        self._stop_task.add_done_callback(cleanup_stop)

    def cancel(self):
        # TODO: test if callbacks run when tasks are canceled
        set_trace()
        if self._stop_task:
            self._stop_task.cancel()
            self._stop_task = None
        if self._main_task:
            self._main_task.cancel()
            self._main_task = None
        for task in self._func_tasks:
            task.cancel()


class Cron(Runner):
    def __init__(
        self,
        func: Callable,
        expr: str,
        n_workers: int = 1,
        n_retries: int = 0,
        eager: bool = False,
    ):
        super().__init__(func, n_workers, n_retries)
        self.expr = expr
        self.eager = eager

        self.cron = croniter(self.expr, datetime.utcnow())

    def __repr__(self):
        return f"Cron({self.func.__name__})"

    def _get_sleep_sec(self) -> float:
        utcnow = datetime.utcnow()
        while True:
            next_dt = self.cron.get_next(datetime)
            if utcnow > next_dt:
                continue
            delta = next_dt - utcnow
            sleep_sec = delta.seconds + (delta.microseconds / 1000000)

            return sleep_sec

    async def _main(self):
        if self.eager:
            await self._func_sem.acquire()
            self._create_func_task()

        while True:
            sleep_sec = self._get_sleep_sec()
            logger.debug(f"Sleeping...", seconds=sleep_sec)
            await asyncio.sleep(sleep_sec)

            while not self._func_sem.locked():
                await self._func_sem.acquire()
                self._create_func_task()


# class SubJob(Job):
#     def __init__(self, *, queue: BaseQueue, **kwargs):
#         super().__init__(**kwargs)
#         self.queue = queue
#
#     def __repr__(self):
#         return f"Sub({self.queue})"
#
#     async def _main(self):
#         while True:
#             await self._worker_sem.acquire()
#
#             # NOTE: polling is infinite with no pauses between polls
#             payload = None
#             while payload is None:
#                 payload = await self.queue.poll()
#
#             self._create_worker_task(payload=payload)
