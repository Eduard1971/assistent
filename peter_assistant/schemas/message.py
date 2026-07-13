from __future__ import annotations
from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel, Field

class ReplyRoute(BaseModel):
    channel: str
    target: str
    metadata: dict[str, Any] = Field(default_factory=dict)

class Message(BaseModel):
    channel: str
    message_id: str
    conversation_id: str
    sender_id: str
    sender_name: str = ""
    recipient_id: str = ""
    subject: str = ""
    body: str = ""
    attachments: list[str] = Field(default_factory=list)
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reply_route: ReplyRoute
    metadata: dict[str, Any] = Field(default_factory=dict)
