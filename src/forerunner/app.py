import asyncio
import signal
from inspect import iscoroutinefunction
from typing import Callable, Iterable, List, Literal, Optional, Union

from structlog import get_logger

from .runner import Cron

logger = get_logger("forerunner")


class Forerunner:
    def __init__(
        self,
        on_startup: Optional[List[Callable]] = None,
        on_shutdown: Optional[List[Callable]] = None,
        runners: Optional[List[Cron]] = None,
    ):
        self.on_startup = on_startup if on_startup else []
        self.on_shutdown = on_shutdown if on_shutdown else []
        self.runners = runners if runners else []

        self._should_exit = False
        self._force_exit = False

    async def startup(self):
        logger.info("Starting...")
        if self.on_startup is not None:
            if isinstance(self.on_startup, list):
                for func in self.on_startup:
                    await func() if iscoroutinefunction(func) else func()
            else:
                await self.on_startup() if iscoroutinefunction(
                    self.on_startup
                ) else self.on_startup()

    async def shutdown(self):
        logger.info("Shutting down...")

        # Stop runners
        for runner in self.runners:
            if runner.is_stopped:
                continue
            runner.stop()
        logger.info("Waiting for Jobs to finish. (CTRL+C to force quit)")
        while not self._force_exit:
            if all([runner.is_stopped for runner in self.runners]):
                break
            await asyncio.sleep(0.1)

        if self._force_exit:
            logger.debug("Force exiting. Canceling all Jobs...")
            for runner in self.runners:
                runner.cancel()

        if self.on_shutdown is not None:
            if isinstance(self.on_shutdown, list):
                logger.debug("Running shutdown funcs...")
                for func in self.on_shutdown:
                    await func() if iscoroutinefunction(func) else func()
            else:
                await self.on_shutdown() if iscoroutinefunction(
                    self.on_shutdown
                ) else self.on_shutdown()

        logger.info("App has stopped")

    async def _main_loop(self):
        logger.info("Running...")

        for runner in self.runners:
            runner.start()

        counter = 0
        while not self._should_exit:
            counter += 1
            counter = counter % 86400

            # Break if all asyncio tasks (of each job) have finished
            if all([runner.is_stopped for runner in self.runners]):
                self._should_exit = True
                break

            await asyncio.sleep(0.1)

    def _handle_exit(self):
        if self._should_exit:
            self._force_exit = True
        else:
            self._should_exit = True

    def _init_signal_handlers(self):
        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, self._handle_exit)
        loop.add_signal_handler(signal.SIGTERM, self._handle_exit)

    async def _run(self):
        self._init_signal_handlers()
        await self.startup()
        await self._main_loop()
        await self.shutdown()

    def run(self):
        asyncio.run(self._run())
