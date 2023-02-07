import asyncio
import signal
from inspect import iscoroutinefunction
from typing import Callable, List, Literal

from ipdb import set_trace
from structlog import get_logger

from .job import Cron
from .module import Module
from .utils import init_module_jobs


class App:
    def __init__(
        self,
        *,
        name: str = "app",
        modules: List[Module] = [],
        exception_callbacks: List[Callable] = [],
    ):
        self.name = name
        self.modules = modules
        self.exception_callbacks = exception_callbacks

        self.logger = get_logger(app=self.name)
        self.jobs = []

        for module in self.modules:
            module_jobs = init_module_jobs(
                app_name=self.name,
                exception_callbacks=self.exception_callbacks,
                module=module,
            )
            self.jobs.extend(module_jobs)

        self.startup_funcs = []
        self.shutdown_funcs = []

        self._should_exit = False
        self._force_exit = False

    def on_startup(self, func: Callable):
        self.startup_funcs.append(func)
        return func

    def on_shutdown(self, func: Callable):
        self.shutdown_funcs.append(func)
        return func

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
            job = Cron(
                func=func,
                job_name=func.__name__,
                app_name=self.name,
                exception_callbacks=exception_callbacks,
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
        raise NotImplementedError

    def sub(self):
        raise NotImplementedError

    async def startup(self):
        self.logger.info("Starting...")
        if len(self.startup_funcs) > 0:
            self.logger.debug("Running startup funcs...")
            for func in self.startup_funcs:
                await func() if iscoroutinefunction(func) else func()

    async def shutdown(self):
        self.logger.info("Shutting down...")
        for job in self.jobs:
            job.stop()

        self.logger.info("Waiting for Jobs to finish. (CTRL+C to force quit)")
        while not self._force_exit:
            if all([job.is_stopped for job in self.jobs]):
                break
            await asyncio.sleep(0.25)

        if self._force_exit:
            self.logger.debug("Force exiting. Canceling all jobs...")
            for job in self.jobs:
                job.cancel()

        if len(self.shutdown_funcs) > 0:
            self.logger.debug("Running shutdown funcs...")
            for func in self.shutdown_funcs:
                await func() if iscoroutinefunction(func) else func()

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

    async def _run(self):
        await self.startup()
        await self._main_loop()
        await self.shutdown()

    def run(self):
        asyncio.run(self._run())
