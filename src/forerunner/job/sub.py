import asyncio

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
            res = await self.queue.get()
            await self._create_worker_task(res)
