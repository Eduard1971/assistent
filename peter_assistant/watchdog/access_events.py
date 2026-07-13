from __future__ import annotations
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

class JsonAccessEventProvider:
    def __init__(self,path:str): self.path=Path(path)
    def recent_authorized_entry(self,camera_id:str,window_seconds:int)->dict|None:
        if not self.path.exists(): return None
        cutoff=datetime.now(timezone.utc)-timedelta(seconds=window_seconds)
        lines=self.path.read_text(encoding="utf-8",errors="ignore").splitlines()[-500:]
        for line in reversed(lines):
            try: item=json.loads(line); ts=datetime.fromisoformat(item["timestamp"])
            except Exception: continue
            if ts>=cutoff and item.get("camera_id")==camera_id and item.get("authorized") is True:
                return item
        return None
