from __future__ import annotations
import re
from schemas.event import Event, EventType
from schemas.message import Message
from .model_router import ModelRouter

class PeterAssistant:
    def __init__(self, models: ModelRouter, supervisor_email: str):
        self.models=models; self.supervisor_email=(supervisor_email or "").lower()

    async def classify(self, message: Message, state: str="new") -> Event:
        body=message.body.upper(); subj=message.subject.upper(); sender=message.sender_id.lower()
        if self.supervisor_email and sender==self.supervisor_email:
            if any(x in body for x in ("SCHVALUJEM","SCHVAĽUJEM","SÚHLASÍM","APPROVED")):
                return Event(type=EventType.SUPERVISOR_APPROVAL,message=message,payload={"offer_number":self._offer_number(subj+"\n"+body)})
            if any(x in body for x in ("ZAMIETAM","ZAMIETA","REJECTED")):
                return Event(type=EventType.SUPERVISOR_REJECTION,message=message,payload={"offer_number":self._offer_number(subj+"\n"+body)})
        if any(x in body+subj for x in ("AKCEPTUJEM PONUKU","SÚHLASÍM S PONUKOU","ACCEPT OFFER")):
            return Event(type=EventType.CLIENT_ACCEPTANCE,message=message,payload={"offer_number":self._offer_number(subj+"\n"+body)})
        if any(x in body+subj for x in ("ODMIETAM PONUKU","NEZÁUJEM","DECLINE OFFER")):
            return Event(type=EventType.CLIENT_REJECTION,message=message,payload={"offer_number":self._offer_number(subj+"\n"+body)})
        return Event(type=EventType.CUSTOMER_REPLY if state!="new" or subj.startswith("RE:") else EventType.NEW_REQUEST,message=message)

    async def detect_service(self, message: Message) -> tuple[str,dict]:
        text=(message.subject+"\n"+message.body).lower()
        synonyms={"housing":["serverhousing","server housing","1u","2u","4u","10u"],"dc_rack":["dc rack","rack housing","celý rack"],"internet":["internet","optigarant","optiflexi","konektivita"],"l2":["l2","dátový okruh","point to point"],"xconnect":["cross connect","xconnect"],"peering":["peering","bgp","transit"]}
        for service,words in synonyms.items():
            if any(w in text for w in words): return service,{}
        data=await self.models.local_json(f"Urči službu a parametre z emailu. Vráť JSON service_type, params. Email: {text[:3000]}","Si obchodný analyzátor Trestel SK.")
        return data.get("service_type","unknown"),data.get("params",{})

    @staticmethod
    def _offer_number(text: str) -> str:
        m=re.search(r"\bPON[-_/ ]?(\d{4})[-_/ ]?(\d{3,6})\b",text.upper())
        return f"PON-{m.group(1)}-{m.group(2)}" if m else ""
