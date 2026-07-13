from __future__ import annotations
import hashlib
import httpx
from .base import Channel
from schemas.message import Message, ReplyRoute

class EmailChannel(Channel):
    name = "email"
    def __init__(self, mcp_url: str):
        self.mcp_url = mcp_url.rstrip("/")

    async def receive(self) -> list[Message]:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{self.mcp_url}/emails")
            response.raise_for_status()
            raw = response.json() or []
        messages: list[Message] = []
        for item in raw:
            sender = (item.get("from_email") or "").strip().lower()
            subject = item.get("subject") or ""
            body = item.get("body") or item.get("body_text") or ""
            mid = item.get("message_id") or item.get("imap_id") or hashlib.sha256(f"{sender}|{subject}|{body}".encode()).hexdigest()
            thread = item.get("thread_id") or f"email:{sender}:{subject.lower().removeprefix('re:').strip()}"
            messages.append(Message(channel="email",message_id=str(mid),conversation_id=thread,sender_id=sender,sender_name=item.get("from_name") or "",recipient_id=item.get("to_email") or "",subject=subject,body=body,attachments=item.get("attachments") or [],reply_route=ReplyRoute(channel="email",target=sender,metadata={"reply_to_id": item.get("message_id") or "", "imap_id": item.get("imap_id") or ""}),metadata=item))
        return messages

    async def send(self, route: ReplyRoute, subject: str, body: str, attachments: list[str] | None = None) -> dict:
        payload = {"to": route.target, "subject": subject, "body_html": body, "attachments": attachments or []}
        if route.metadata.get("reply_to_id"):
            payload["reply_to_id"] = route.metadata["reply_to_id"]
        async with httpx.AsyncClient(timeout=45) as client:
            response = await client.post(f"{self.mcp_url}/tool/send_email", json=payload)
            response.raise_for_status()
            return response.json() if response.content else {"ok": True}

    async def mark_processed(self, route: ReplyRoute) -> None:
        imap_id = route.metadata.get("imap_id")
        if not imap_id:
            return
        async with httpx.AsyncClient(timeout=20) as client:
            await client.post(f"{self.mcp_url}/tool/mark_email_processed", json={"imap_id": imap_id})
