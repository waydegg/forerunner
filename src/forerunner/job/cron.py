import asyncio
import inspect
import sys
import traceback
from datetime import datetime
from typing import Callable, List, Literal, cast

import structlog
from croniter import croniter
from ipdb import set_trace

from .base import Job

logger = structlog.get_logger()


class Cron(Job):
    def __init__(self, *, expr: str, eager: bool, **kwargs):
        super().__init__(**kwargs)
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
            task = self._create_worker_task()
            if task is not None:
                try:
                    await task
                except:
                    pass  # Do nothing; let task callback handle Exception

        while True:
            sleep_sec = self._get_sleep_sec()
            self.logger.debug(f"Sleeping {sleep_sec} seconds...")
            await asyncio.sleep(sleep_sec)

            match self.strategy:
                case "burst":
                    while len(self._worker_tasks) < self.n_workers:
                        self._create_worker_task()
                case "overlap":
                    raise NotImplementedError
