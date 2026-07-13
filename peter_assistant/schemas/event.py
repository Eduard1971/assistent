from __future__ import annotations
from enum import StrEnum
from typing import Any
from pydantic import BaseModel, Field
from .message import Message

class EventType(StrEnum):
    NEW_REQUEST = "new_request"
    CUSTOMER_REPLY = "customer_reply"
    SUPERVISOR_APPROVAL = "supervisor_approval"
    SUPERVISOR_REJECTION = "supervisor_rejection"
    CLIENT_ACCEPTANCE = "client_acceptance"
    CLIENT_REJECTION = "client_rejection"
    UNKNOWN = "unknown"
    WATCHDOG_PERSON = "watchdog_person"

class Event(BaseModel):
    type: EventType
    message: Message
    confidence: float = 1.0
    payload: dict[str, Any] = Field(default_factory=dict)
