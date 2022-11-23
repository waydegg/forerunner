import asyncio
import signal
from typing import Callable, List, Literal

import structlog
from ipdb import set_trace

from .job import Cron

logger = structlog.get_logger()


class App:
    def __init__(
        self, *, name: str = "app", global_exception_callbacks: List[Callable] = []
    ):
        self.name = name
        self.global_exception_callbacks = global_exception_callbacks

        self.logger = logger.bind(app_name=self.name)
        self.jobs: List[Cron] = []

        self.startup_tasks = []
        self.shutdown_tasks = []

        self._should_exit = False
        self._force_exit = False

    def on_event(self, event: Literal["startup", "shutdown"]):
        def _on_event_wrapper(func: Callable):
            match event:
                case "startup":
                    self.startup_tasks.append(func)
                case "shutdown":
                    self.shutdown_tasks.append(func)
            return func

        return _on_event_wrapper

    def cron(
        self,
        expr: str,
        *,
        n_workers: int = 1,
        n_retries: int = 0,
        execution: Literal["sync", "async", "thread", "process"] = "async",
        eager: bool = False,
        strategy: Literal["burst", "overlap"] = "burst",
        exception_callbacks: List[Callable] = []
    ):
        def _cron_wrapper(func: Callable):
            job = Cron(
                func=func,
                job_name=func.__name__,
                app_name=self.name,
                exception_callbacks=(
                    self.global_exception_callbacks + exception_callbacks
                ),
                expr=expr,
                n_workers=n_workers,
                n_retries=n_retries,
                execution=execution,
                eager=eager,
                strategy=strategy,
            )
            self.jobs.append(job)
            return func

        return _cron_wrapper

    def timer(self):
        ...

    def sub(self):
        ...

    async def _main_loop(self):
        self.logger.info("Running...")
        self._init_signal_handlers()

        for job in self.jobs:
            job.start()

        counter = 0
        while not self._should_exit:
            counter += 1
            counter = counter % 86400

            # Break if all asyncio tasks (of each job) have finished
            if all([j.is_stopped for j in self.jobs]):
                self._should_exit = True
                break

            await asyncio.sleep(0.1)

    def _handle_exit(self, sig):
        if self._should_exit:
            self._force_exit = True
        else:
            self._should_exit = True

    def _init_signal_handlers(self):
        loop = asyncio.get_event_loop()
        for sig in [signal.SIGINT, signal.SIGTERM]:
            loop.add_signal_handler(sig, self._handle_exit, sig)

    async def startup(self):
        self.logger.info("Starting...")

        if self.startup_tasks:
            self.logger.info("Running starup tasks...")
            await asyncio.gather(*self.startup_tasks)

    async def shutdown(self):
        self.logger.info("Shutting down...")
        for job in self.jobs:
            job.stop()

        self.logger.info("Waiting for Jobs to finish. (CTRL+C to force quit)")
        while not self._force_exit:
            if all([job.is_stopped for job in self.jobs]):
                break
            await asyncio.sleep(1)

        if self._force_exit:
            self.logger.debug("Force exiting. Canceling all jobs...")
            for job in self.jobs:
                job.cancel()

        if self.shutdown_tasks:
            self.logger.info("Running shutdown tasks...")
            await asyncio.gather(*self.shutdown_tasks)

    async def _run(self):
        await self.startup()
        await self._main_loop()
        await self.shutdown()

    def run(self):
        asyncio.run(self._run())