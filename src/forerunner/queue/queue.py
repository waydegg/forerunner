import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class BasePayload(ABC):
    ack_id: Any


class BaseQueue(ABC):
    @abstractmethod
    async def poll(self) -> BasePayload | None:
        ...

    @abstractmethod
    async def push(self, payload):
        ...

    @abstractmethod
    async def ack(self, payload):
        ...


@dataclass
class AsyncQueuePayload(BasePayload):
    ack_id: None
    data: Any


class AsyncQueue(BaseQueue):
    def __init__(self, name: str | None = None):
        self.name = name
        self.queue = asyncio.Queue()

    async def poll(self):
        res = await self.queue.get()
        payload = AsyncQueuePayload(ack_id=None, data=res)

        return payload

    async def push(self, payload):
        await self.queue.put(payload)

    async def ack(self, _):
        pass
