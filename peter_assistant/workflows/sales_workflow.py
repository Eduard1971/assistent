from __future__ import annotations
from schemas.event import Event, EventType
from schemas.message import ReplyRoute

class SalesWorkflow:
    def __init__(self, db, channels, assistant):
        self.db=db; self.channels=channels; self.assistant=assistant

    async def handle(self, event: Event) -> dict:
        m=event.message; conv=self.db.get_conversation(m.conversation_id)
        state=conv.get("state","new")
        if event.type==EventType.SUPERVISOR_APPROVAL:
            return await self._approval(event)
        if event.type==EventType.SUPERVISOR_REJECTION:
            return self._set_state(m,"manual_review")
        if event.type==EventType.CLIENT_ACCEPTANCE:
            return self._set_state(m,"contract_pending_info")
        if event.type==EventType.CLIENT_REJECTION:
            return self._set_state(m,"closed")
        service,params=await self.assistant.detect_service(m)
        data=(conv.get("data") or {}) | params
        if service=="unknown":
            await self.channels.send(m.reply_route,"Re: "+m.subject,"Dobrý deň,<br><br>ďakujeme za správu. Prosíme, rozpíšte stručne, o akú službu máte záujem, lokalitu a požadované technické parametre.<br><br>S pozdravom<br>Peter")
            self.db.upsert_conversation(m.conversation_id,m.sender_id,m.channel,"pending_info",service,data)
            self.db.add_learning_item(m.body,{"conversation_id":m.conversation_id,"channel":m.channel})
            return {"ok":True,"state":"pending_info"}
        self.db.upsert_conversation(m.conversation_id,m.sender_id,m.channel,"pending_info",service,data)
        await self.channels.send(m.reply_route,"Re: "+m.subject,f"Dobrý deň,<br><br>evidujeme záujem o službu <b>{service}</b>. Prosíme o doplnenie potrebných technických parametrov; následne pripravíme cenovú ponuku.<br><br>S pozdravom<br>Peter")
        return {"ok":True,"state":"pending_info","service_type":service}

    async def _approval(self,event:Event)->dict:
        offer=event.payload.get("offer_number","")
        if not offer: return {"ok":False,"error":"missing offer number"}
        # plný offer repository sa doplní v ďalšej fáze; zatiaľ bezpečne manual review
        return {"ok":False,"state":"manual_review","offer_number":offer,"error":"Offer repository adapter not migrated yet"}

    def _set_state(self,m,state):
        conv=self.db.get_conversation(m.conversation_id)
        self.db.upsert_conversation(m.conversation_id,m.sender_id,m.channel,state,conv.get("service_type","") if conv else "",conv.get("data",{}) if conv else {})
        return {"ok":True,"state":state}
