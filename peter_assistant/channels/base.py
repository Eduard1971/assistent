from __future__ import annotations
from abc import ABC, abstractmethod
from typing import AsyncIterator
from schemas.message import Message, ReplyRoute

class Channel(ABC):
    name: str
    @abstractmethod
    async def receive(self) -> list[Message]: ...
    @abstractmethod
    async def send(self, route: ReplyRoute, subject: str, body: str, attachments: list[str] | None = None) -> dict: ...
