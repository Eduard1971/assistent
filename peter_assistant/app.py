from __future__ import annotations
import asyncio, logging, os, sys
from logging.handlers import RotatingFileHandler
from fastapi import FastAPI
import uvicorn
from config_loader import load_config
from storage.database import Database
from channels.email_channel import EmailChannel
from channels.telegram_channel import TelegramChannel
from channels.whatsapp_channel import WhatsAppChannel
from gateway.outbound_router import OutboundRouter
from gateway.gateway import Gateway
from assistant.model_router import ModelRouter
from assistant.peter import PeterAssistant
from workflows.sales_workflow import SalesWorkflow
from schemas.message import ReplyRoute
from watchdog.access_events import JsonAccessEventProvider
from watchdog.camera_watchdog import CameraWatchdog

async def main():
    cfg=load_config(os.getenv("PETER_CONFIG","config/peter.yaml"))
    log_file=cfg["app"]["log_file"]; os.makedirs(os.path.dirname(log_file),exist_ok=True)
    logging.basicConfig(level=logging.INFO,handlers=[logging.StreamHandler(),RotatingFileHandler(log_file,maxBytes=5_000_000,backupCount=5,encoding="utf-8")],format="%(asctime)s [%(name)s] %(levelname)s - %(message)s")
    db=Database(cfg["app"]["database"])
    channels={}
    if cfg["channels"]["email"]["enabled"]: channels["email"]=EmailChannel(cfg["channels"]["email"]["mcp_url"])
    if cfg["channels"]["telegram"]["enabled"]: channels["telegram"]=TelegramChannel(cfg["channels"]["telegram"]["bot_token"])
    if cfg["channels"]["whatsapp"]["enabled"]: channels["whatsapp"]=WhatsAppChannel(cfg["channels"]["whatsapp"]["access_token"],cfg["channels"]["whatsapp"]["phone_number_id"])
    outbound=OutboundRouter(channels)
    models=ModelRouter(cfg["models"]["local"]["url"],cfg["models"]["local"]["model"])
    assistant=PeterAssistant(models,cfg["sales"].get("supervisor_email", ""))
    workflow=SalesWorkflow(db,outbound,assistant)
    gateway=Gateway(db,assistant,workflow,channels,outbound,cfg["app"]["scan_interval_seconds"])
    tasks=[asyncio.create_task(gateway.run_forever())]
    if cfg.get("watchdog",{}).get("enabled"):
        access=JsonAccessEventProvider(cfg["watchdog"]["access_events"]["path"])
        routes=[]
        target=cfg["watchdog"]["alerts"].get("email_to","")
        if target and "email" in channels: routes.append(ReplyRoute(channel="email",target=target))
        wd=CameraWatchdog(db,outbound,access,cfg["watchdog"]["cameras"],routes,cfg["watchdog"]["allowed_presence_window_seconds"],cfg["watchdog"]["poll_seconds"])
        tasks.append(asyncio.create_task(wd.run_forever()))
    await asyncio.gather(*tasks)

if __name__=="__main__":
    asyncio.run(main())
