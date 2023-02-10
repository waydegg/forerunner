import asyncio
from asyncio.exceptions import TimeoutError

from ipdb import set_trace

from .base import Job


class Sub(Job):
    def __init__(self, *, queue: asyncio.Queue, **kwargs):
        super().__init__(**kwargs)
        self.queue = queue

    def __repr__(self):
        return f"Sub({self.queue})"

    async def _main(self):
        while True:
            while len(self._worker_tasks) >= self.n_workers:
                print("waiting for available worker...")
                await asyncio.sleep(0.5)

            # res = await self.queue.get()
            #
            # await self._create_worker_task(res)

            try:
                res = await asyncio.wait_for(self.queue.get(), timeout=5)
                await self._create_worker_task(res)
            except TimeoutError:
                print("nothing in queue")
                await asyncio.sleep(1)
                continue
