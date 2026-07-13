from __future__ import annotations
import httpx
from .base import Channel
from schemas.message import Message, ReplyRoute

class TelegramChannel(Channel):
    name = "telegram"
    def __init__(self, bot_token: str):
        self.base = f"https://api.telegram.org/bot{bot_token}"
        self.offset = 0
    async def receive(self) -> list[Message]:
        async with httpx.AsyncClient(timeout=35) as c:
            r = await c.get(f"{self.base}/getUpdates", params={"offset": self.offset, "timeout": 1})
            r.raise_for_status()
            updates = r.json().get("result", [])
        out=[]
        for u in updates:
            self.offset=max(self.offset,u["update_id"]+1)
            m=u.get("message") or {}
            if not m: continue
            chat=str((m.get("chat") or {}).get("id",""))
            out.append(Message(channel="telegram",message_id=str(m.get("message_id")),conversation_id=f"telegram:{chat}",sender_id=str((m.get("from") or {}).get("id","")),sender_name=(m.get("from") or {}).get("first_name","") ,body=m.get("text","") ,reply_route=ReplyRoute(channel="telegram",target=chat)))
        return out
    async def send(self, route: ReplyRoute, subject: str, body: str, attachments: list[str] | None=None) -> dict:
        text=(subject+"\n\n"+body).strip()
        async with httpx.AsyncClient(timeout=30) as c:
            r=await c.post(f"{self.base}/sendMessage",json={"chat_id":route.target,"text":text})
            r.raise_for_status(); return r.json()
