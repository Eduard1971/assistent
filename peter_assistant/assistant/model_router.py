from __future__ import annotations
import json, os, re
import httpx

class ModelRouter:
    def __init__(self, ollama_url: str, ollama_model: str):
        self.url=ollama_url.rstrip("/"); self.model=ollama_model
    async def local_json(self, prompt: str, system: str="") -> dict:
        payload={"model":self.model,"prompt":prompt,"stream":False,"format":"json"}
        if system: payload["system"]=system
        try:
            async with httpx.AsyncClient(timeout=90) as c:
                r=await c.post(f"{self.url}/api/generate",json=payload); r.raise_for_status()
            text=r.json().get("response","")
            return json.loads(text)
        except Exception:
            return {}
