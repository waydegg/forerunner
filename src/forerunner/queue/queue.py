import asyncio
from abc import ABC
from dataclasses import dataclass
from typing import Any


class AsyncQueue(asyncio.Queue):
    def __init__(self, name: str | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = name

    def __repr__(self):
        memory_addr = hex(id(self))
        parts = [f"size={self.qsize()}", f"maxsize={self.maxsize}"]
        parts_str = " ".join(parts)

        return f"<AsyncQueue({self.name or ''}) at {memory_addr} ({parts_str})>"


# class BasePayload(ABC):
#     @property
#     def ack_id(self) -> Any:
#         ...


@dataclass
class BasePayload:
    ack_id: Any


class BaseQueue:
    def __init__(self):
        ...

    async def poll(self) -> BasePayload | None:
        ...

    async def push(self, payload):
        ...

    async def ack(self, payload):
        ...
