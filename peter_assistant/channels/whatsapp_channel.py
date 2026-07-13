from __future__ import annotations
import httpx
from .base import Channel
from schemas.message import Message, ReplyRoute

class WhatsAppChannel(Channel):
    name = "whatsapp"
    def __init__(self, access_token: str, phone_number_id: str):
        self.token=access_token; self.phone_number_id=phone_number_id
    async def receive(self) -> list[Message]:
        return []  # inbound ide cez webhook endpoint; gateway ho vloží do fronty
    async def send(self, route: ReplyRoute, subject: str, body: str, attachments: list[str] | None=None) -> dict:
        url=f"https://graph.facebook.com/v20.0/{self.phone_number_id}/messages"
        headers={"Authorization":f"Bearer {self.token}"}
        payload={"messaging_product":"whatsapp","to":route.target,"type":"text","text":{"body":(subject+"\n\n"+body).strip()}}
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post(url,headers=headers,json=payload); r.raise_for_status(); return r.json()
