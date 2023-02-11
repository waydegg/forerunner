from contextlib import AsyncExitStack

from forerunner.dependency.utils import resolve_dependencies
from forerunner.queue.queue import BaseQueue

from .base import Job


class Sub(Job):
    def __init__(self, *, queue: BaseQueue, **kwargs):
        super().__init__(**kwargs)
        self.queue = queue

    def __repr__(self):
        return f"Sub({self.queue})"

    async def _main(self):
        while True:
            await self._worker_sem.acquire()

            # NOTE: polling is infinite with no pauses between polls
            payload = None
            while payload is None:
                payload = await self.queue.poll()

            self._create_worker_task(payload=payload)
