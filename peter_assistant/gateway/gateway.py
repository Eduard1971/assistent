from __future__ import annotations
import asyncio, logging
from fastapi import FastAPI
from schemas.message import Message

log=logging.getLogger("peter.gateway")

class Gateway:
    def __init__(self,db,assistant,workflow,channels,outbound,poll_seconds:int=60):
        self.db=db; self.assistant=assistant; self.workflow=workflow; self.channels=channels; self.outbound=outbound; self.poll_seconds=poll_seconds; self.running=True

    async def process(self,message:Message):
        if self.db.is_processed(message.channel,message.message_id): return {"ok":True,"duplicate":True}
        conv=self.db.get_conversation(message.conversation_id)
        self.db.add_message(message.conversation_id,"in",message.channel,message.message_id,message.subject,message.body)
        event=await self.assistant.classify(message,conv.get("state","new") if conv else "new")
        result=await self.workflow.handle(event)
        self.db.mark_processed(message.channel,message.message_id)
        ch=self.channels.get(message.channel)
        if hasattr(ch,"mark_processed"): await ch.mark_processed(message.reply_route)
        return result

    async def run_forever(self):
        while self.running:
            for channel in self.channels.values():
                try:
                    for message in await channel.receive(): await self.process(message)
                except Exception: log.exception("Channel cycle failed: %s",getattr(channel,"name","unknown"))
            await asyncio.sleep(self.poll_seconds)
