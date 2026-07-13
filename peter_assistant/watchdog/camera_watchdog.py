from __future__ import annotations
import asyncio, logging
from datetime import datetime
from pathlib import Path
import cv2
from schemas.message import ReplyRoute

log=logging.getLogger("peter.watchdog")

class CameraWatchdog:
    """Deteguje osobu a koreluje ju s prístupovou udalosťou. Nerobí rozpoznávanie tváre."""
    def __init__(self,db,outbound,access_provider,cameras,alert_routes,window_seconds=45,poll_seconds=2):
        self.db=db; self.outbound=outbound; self.access=access_provider; self.cameras=cameras; self.alert_routes=alert_routes; self.window=window_seconds; self.poll=poll_seconds
        self.detector=cv2.HOGDescriptor(); self.detector.setSVMDetector(cv2.HOGDescriptor_getDefaultPeopleDetector())
        self.last_alert={}

    async def run_forever(self):
        while True:
            for cam in self.cameras:
                if not cam.get("enabled"): continue
                await self._check(cam)
            await asyncio.sleep(self.poll)

    async def _check(self,cam):
        cap=cv2.VideoCapture(cam.get("source"))
        ok,frame=cap.read(); cap.release()
        if not ok: return
        boxes,_=self.detector.detectMultiScale(frame,winStride=(8,8),padding=(8,8),scale=1.05)
        if len(boxes)==0: return
        now=datetime.now().timestamp(); last=self.last_alert.get(cam["id"],0)
        if now-last<30: return
        access=self.access.recent_authorized_entry(cam["id"],self.window)
        authorized=bool(access)
        evidence_dir=Path("data/watchdog_evidence"); evidence_dir.mkdir(parents=True,exist_ok=True)
        evidence=evidence_dir/f"{cam['id']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"; cv2.imwrite(str(evidence),frame)
        self.db.add_watchdog_event(cam["id"],"person_detected",authorized,str(evidence),{"access_event":access or {}})
        status="POVOLENÝ VSTUP" if authorized else "NEOVERENÝ VSTUP"
        body=f"Kamera: {cam.get('name',cam['id'])}<br>Stav: <b>{status}</b><br>Čas: {datetime.now().isoformat(timespec='seconds')}"
        for route in self.alert_routes:
            await self.outbound.send(route,f"Watchdog – {status}",body,[str(evidence)])
        self.last_alert[cam["id"]]=now
