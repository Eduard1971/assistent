#!/usr/bin/env python3
"""
Peter Gateway Server v2.0
=========================
Centrálny MCP/REST server pre všetkých budúcich asistentov Trestel/SITEL.

Úlohy servera:
- správa komunikačných kanálov (aktuálne email; ďalšie cez adaptéry),
- registrácia asistentov a heartbeat,
- spoločný event bus medzi asistentmi,
- jednotný message log a deduplikácia,
- ponuky a zmluvy,
- watchdog udalosti z kamier a prístupového systému,
- MCP nástroje aj REST API,
- spätná kompatibilita s pôvodným Sales MCP serverom.

Bezpečnostný model:
- voliteľný Bearer/X-API-Key cez GATEWAY_API_KEY,
- idempotency_key pri eventoch a správach,
- kritické business prechody ostávajú v deterministických workflowoch klientov,
- server je control plane, nie LLM agent.
"""

from __future__ import annotations

import email as email_lib
import email.header
import hashlib
import imaplib
import json
import logging
import mimetypes
import os
import re
import smtplib
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from email.message import EmailMessage
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import uvicorn
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.sse import SseServerTransport
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_env(env_path: str = ".env") -> None:
    candidates = [Path(env_path), Path(__file__).resolve().parent / env_path, Path(__file__).resolve().parents[1] / env_path]
    for path in candidates:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                key, _, value = s.partition("=")
                key, value = key.strip(), value.strip()
                if value and value[0] not in ('"', "'") and "#" in value:
                    value = value.split("#", 1)[0].rstrip()
                if len(value) >= 2 and value[0] in ('"', "'") and value[-1] == value[0]:
                    value = value[1:-1]
                if key and key not in os.environ:
                    os.environ[key] = value
        break


_load_env()

HOST = os.getenv("GATEWAY_HOST", os.getenv("MCP_HOST", "0.0.0.0"))
PORT = int(os.getenv("GATEWAY_PORT", os.getenv("MCP_PORT", "8082")))
API_KEY = os.getenv("GATEWAY_API_KEY", "").strip()
SERVER_NAME = os.getenv("GATEWAY_NAME", "peter-gateway")
DB_PATH = Path(os.getenv("GATEWAY_DB_PATH", os.getenv("DB_PATH", "C:/AI/salesperson/contracts.db")))
OFFERS_DIR = Path(os.getenv("OFFERS_DIR", "C:/AI/salesperson/offers"))

SALES_EMAIL = os.getenv("SALES_EMAIL", "enemec@trestel.sk").strip()
EMAIL_PASSWORD = os.getenv("SALES_EMAIL_PASSWORD", "")
IMAP_HOST = os.getenv("IMAP_HOST", "imap.gmail.com")
IMAP_PORT = int(os.getenv("IMAP_PORT", "993"))
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SUPERVISOR_EMAIL = os.getenv("SUPERVISOR_EMAIL", "")


def _build_mailboxes() -> List[Dict[str, Any]]:
    """Zoznam schránok, ktoré server sleduje/z ktorých odosiela. 'sales' je vždy prítomná
    (existujúce SALES_EMAIL/* premenné). Ďalšie schránky (napr. FLM hotline) sa pridajú, ak
    majú nastavený vlastný e-mail + heslo - inak sa jednoducho nepollujú, nič nezlyhá."""
    boxes = [{
        "id": "sales",
        "email": SALES_EMAIL,
        "password": EMAIL_PASSWORD,
        "imap_host": IMAP_HOST,
        "imap_port": IMAP_PORT,
        "smtp_host": SMTP_HOST,
        "smtp_port": SMTP_PORT,
    }]
    flm_email = os.getenv("FLM_EMAIL", "").strip()
    flm_password = os.getenv("FLM_EMAIL_PASSWORD", "")
    if flm_email and flm_password:
        boxes.append({
            "id": "flm",
            "email": flm_email,
            "password": flm_password,
            "imap_host": os.getenv("FLM_IMAP_HOST", IMAP_HOST),
            "imap_port": int(os.getenv("FLM_IMAP_PORT", str(IMAP_PORT))),
            "smtp_host": os.getenv("FLM_SMTP_HOST", SMTP_HOST),
            "smtp_port": int(os.getenv("FLM_SMTP_PORT", str(SMTP_PORT))),
        })
    return boxes


MAILBOXES = _build_mailboxes()
MAILBOXES_BY_ID = {b["id"]: b for b in MAILBOXES}

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [Peter-Gateway] %(levelname)s - %(message)s",
)
log = logging.getLogger("peter_gateway")

_DB_LOCK = threading.RLock()


@contextmanager
def db_conn():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _DB_LOCK:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def init_db() -> None:
    with db_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                name TEXT,
                company TEXT,
                phone TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                offer_number TEXT UNIQUE,
                customer_email TEXT,
                customer_name TEXT,
                company TEXT,
                service_type TEXT,
                status TEXT DEFAULT 'draft',
                total_monthly_eur REAL DEFAULT 0,
                total_setup_eur REAL DEFAULT 0,
                offer_folder TEXT,
                email_thread_id TEXT,
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now')),
                supervisor_approved INTEGER DEFAULT 0,
                client_accepted INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS contracts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                contract_number TEXT UNIQUE,
                offer_id INTEGER REFERENCES offers(id),
                customer_email TEXT,
                customer_name TEXT,
                company TEXT,
                ico TEXT,
                ic_dph TEXT,
                address TEXT,
                service_description TEXT,
                monthly_fee_eur REAL DEFAULT 0,
                setup_fee_eur REAL DEFAULT 0,
                contract_duration_months INTEGER DEFAULT 12,
                signed_date TEXT,
                start_date TEXT,
                status TEXT DEFAULT 'draft',
                contract_file TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS agents (
                agent_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                version TEXT,
                capabilities_json TEXT DEFAULT '[]',
                metadata_json TEXT DEFAULT '{}',
                status TEXT DEFAULT 'online',
                registered_at TEXT,
                last_heartbeat TEXT
            );

            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                source_agent TEXT,
                target_agent TEXT,
                conversation_id TEXT,
                correlation_id TEXT,
                payload_json TEXT NOT NULL,
                priority INTEGER DEFAULT 5,
                status TEXT DEFAULT 'pending',
                idempotency_key TEXT UNIQUE,
                created_at TEXT,
                available_at TEXT,
                claimed_at TEXT,
                acknowledged_at TEXT,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_events_target_status
                ON events(target_agent, status, available_at, priority, created_at);

            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_key TEXT UNIQUE,
                channel TEXT NOT NULL,
                direction TEXT NOT NULL,
                message_id TEXT,
                conversation_id TEXT,
                sender_id TEXT,
                recipient_id TEXT,
                subject TEXT,
                body TEXT,
                attachments_json TEXT DEFAULT '[]',
                metadata_json TEXT DEFAULT '{}',
                status TEXT DEFAULT 'received',
                created_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_messages_conversation
                ON messages(conversation_id, created_at);

            CREATE TABLE IF NOT EXISTS access_events (
                event_id TEXT PRIMARY KEY,
                source TEXT,
                camera_id TEXT,
                person_ref TEXT,
                authorized INTEGER NOT NULL,
                occurred_at TEXT NOT NULL,
                metadata_json TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS watchdog_events (
                event_id TEXT PRIMARY KEY,
                camera_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                authorization_status TEXT NOT NULL,
                person_ref TEXT,
                snapshot_path TEXT,
                confidence REAL,
                metadata_json TEXT DEFAULT '{}',
                occurred_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notifications (
                notification_id TEXT PRIMARY KEY,
                event_id TEXT,
                channel TEXT,
                target TEXT,
                subject TEXT,
                body TEXT,
                status TEXT DEFAULT 'pending',
                error TEXT,
                created_at TEXT,
                sent_at TEXT
            );

            CREATE TABLE IF NOT EXISTS email_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                direction TEXT,
                from_addr TEXT,
                to_addr TEXT,
                subject TEXT,
                body_preview TEXT,
                offer_id INTEGER,
                sent_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
    log.info("Gateway DB inicializovaná: %s", DB_PATH)


init_db()


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def json_load(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def decode_header_value(value: str) -> str:
    result: List[str] = []
    for part, charset in email.header.decode_header(value or ""):
        if isinstance(part, bytes):
            try:
                result.append(part.decode(charset or "utf-8", errors="replace"))
            except Exception:
                result.append(part.decode("latin1", errors="replace"))
        else:
            result.append(part)
    return "".join(result)


def extract_email_addresses(header_value: str) -> List[str]:
    if not header_value:
        return []
    result: List[str] = []
    for part in header_value.split(","):
        match = re.search(r"<([^>]+)>", part.strip())
        address = match.group(1).strip() if match else part.strip()
        if "@" in address:
            result.append(address.lower())
    return result


def get_email_body(msg: Any) -> str:
    body = ""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            charset = part.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if ctype == "text/plain":
                body = text
                break
            if ctype == "text/html" and not body:
                text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
                text = re.sub(r"</p\s*>", "\n\n", text, flags=re.I)
                body = re.sub(r"<[^>]+>", " ", text)
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            body = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
    return re.sub(r"\n{3,}", "\n\n", body).strip()


def make_message_key(channel: str, message_id: str, fallback: str = "") -> str:
    raw = f"{channel}|{message_id or fallback}"
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def store_message(message: Dict[str, Any]) -> bool:
    key = message.get("message_key") or make_message_key(
        str(message.get("channel", "unknown")),
        str(message.get("message_id", "")),
        str(message.get("fallback_key", uuid.uuid4())),
    )
    try:
        with db_conn() as conn:
            conn.execute(
                """
                INSERT INTO messages
                (message_key, channel, direction, message_id, conversation_id,
                 sender_id, recipient_id, subject, body, attachments_json,
                 metadata_json, status, created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    key,
                    message.get("channel", "unknown"),
                    message.get("direction", "in"),
                    message.get("message_id", ""),
                    message.get("conversation_id", ""),
                    message.get("sender_id", ""),
                    message.get("recipient_id", ""),
                    message.get("subject", ""),
                    message.get("body", ""),
                    json_text(message.get("attachments", [])),
                    json_text(message.get("metadata", {})),
                    message.get("status", "received"),
                    message.get("created_at", utc_now()),
                ),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def fetch_mailbox_emails(mailbox: Dict[str, Any], max_count: int) -> List[Dict[str, Any]]:
    box_id = mailbox["id"]
    try:
        mail = imaplib.IMAP4_SSL(mailbox["imap_host"], mailbox["imap_port"])
        mail.login(mailbox["email"], mailbox["password"])
        mail.select("INBOX")
        _, ids = mail.search(None, "UNSEEN")
        email_ids = ids[0].split() if ids[0] else []
        log.info("IMAP[%s]: %s nových emailov", box_id, len(email_ids))
        results: List[Dict[str, Any]] = []
        for eid in email_ids[-max_count:]:
            _, data = mail.fetch(eid, "(BODY.PEEK[])")
            if not data or not data[0] or not isinstance(data[0], tuple):
                continue
            msg = email_lib.message_from_bytes(data[0][1])
            from_raw = decode_header_value(msg.get("From", ""))
            match = re.search(r"<([^>]+)>", from_raw)
            from_email = (match.group(1) if match else from_raw).strip().lower()
            subject = decode_header_value(msg.get("Subject", "(bez predmetu)"))
            message_id = msg.get("Message-ID", "")
            in_reply_to = msg.get("In-Reply-To", "")
            references = msg.get("References", "")
            thread_id = in_reply_to or (references.split()[-1] if references else message_id)
            body = get_email_body(msg)
            to_raw = decode_header_value(msg.get("To", ""))
            cc_raw = decode_header_value(msg.get("Cc", ""))
            item = {
                "imap_id": eid.decode(),
                "mailbox": box_id,
                "from": from_raw,
                "from_email": from_email,
                "to_raw": to_raw,
                "cc_raw": cc_raw,
                "to_list": extract_email_addresses(to_raw),
                "cc_list": extract_email_addresses(cc_raw),
                "subject": subject,
                "date": msg.get("Date", ""),
                "message_id": message_id,
                "thread_id": thread_id,
                "body": body[:10000],
                "body_length": len(body),
                "channel": "email",
                "conversation_id": f"email:{thread_id or from_email}",
            }
            results.append(item)
            store_message({
                "channel": "email",
                "direction": "in",
                "message_id": message_id or f"imap:{box_id}:{eid.decode()}",
                "conversation_id": item["conversation_id"],
                "sender_id": from_email,
                "recipient_id": mailbox["email"],
                "subject": subject,
                "body": body,
                "metadata": {"mailbox": box_id, "imap_id": eid.decode(), "thread_id": thread_id, "to": item["to_list"], "cc": item["cc_list"]},
                "created_at": utc_now(),
            })
        mail.logout()
        return results
    except imaplib.IMAP4.error as exc:
        log.error("IMAP[%s] chyba: %s", box_id, exc)
        return [{"error": f"IMAP login zlyhal ({box_id}): {exc}", "mailbox": box_id}]
    except Exception as exc:
        log.exception("Email fetch chyba (%s)", box_id)
        return [{"error": str(exc), "mailbox": box_id}]


def fetch_new_emails(max_count: int = 20) -> List[Dict[str, Any]]:
    active = [b for b in MAILBOXES if b["password"]]
    if not active:
        return [{"error": "Žiadna schránka nemá nastavené heslo (SALES_EMAIL_PASSWORD / FLM_EMAIL_PASSWORD) v .env"}]
    results: List[Dict[str, Any]] = []
    for mailbox in active:
        results.extend(fetch_mailbox_emails(mailbox, max_count))
    return results


def mark_email_read(imap_id: str, mailbox: str = "sales") -> bool:
    box = MAILBOXES_BY_ID.get(mailbox)
    if not box:
        log.error("Mark email processed: neznáma schránka '%s'", mailbox)
        return False
    try:
        mail = imaplib.IMAP4_SSL(box["imap_host"], box["imap_port"])
        mail.login(box["email"], box["password"])
        mail.select("INBOX")
        mail.store(str(imap_id), "+FLAGS", "\\Seen")
        mail.logout()
        return True
    except Exception as exc:
        log.error("Mark email processed chyba (%s): %s", mailbox, exc)
        return False


def send_email_smtp(
    to: str,
    subject: str,
    body_html: str,
    body_text: str = "",
    reply_to_id: str = "",
    attachments: Optional[List[str]] = None,
    cc: Optional[List[str]] = None,
    conversation_id: str = "",
    idempotency_key: str = "",
    mailbox: str = "sales",
) -> Dict[str, Any]:
    box = MAILBOXES_BY_ID.get(mailbox) or MAILBOXES_BY_ID["sales"]
    from_email = box["email"]
    if not box["password"]:
        return {"ok": False, "error": f"Heslo pre schránku '{mailbox}' nie je nastavené v .env"}
    attachments = attachments or []
    cc_list = [x for x in (cc or []) if x and x.lower() not in {from_email.lower(), to.lower()}]
    message_key = idempotency_key or make_message_key("email-out", "", f"{to}|{subject}|{body_text or body_html}|{attachments}")
    with db_conn() as conn:
        exists = conn.execute("SELECT 1 FROM messages WHERE message_key=?", (message_key,)).fetchone()
    if exists:
        return {"ok": True, "duplicate": True, "message_key": message_key}
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to
        if cc_list:
            msg["Cc"] = ", ".join(cc_list)
        if reply_to_id:
            msg["In-Reply-To"] = reply_to_id
            msg["References"] = reply_to_id
        msg["X-Mailer"] = "Peter-Gateway/2.0"
        plain = body_text.strip() if body_text else ""
        if not plain:
            plain = re.sub(r"<br\s*/?>", "\n", body_html or "", flags=re.I)
            plain = re.sub(r"</p\s*>", "\n\n", plain, flags=re.I)
            plain = re.sub(r"<[^>]+>", "", plain)
            plain = re.sub(r"\n{3,}", "\n\n", plain).strip()
        msg.set_content(plain or "Správa je v HTML formáte.")
        if body_html:
            msg.add_alternative(body_html, subtype="html")
        attached, missing = [], []
        for file_name in attachments:
            path = Path(str(file_name))
            if not path.exists() or not path.is_file():
                missing.append(str(path))
                continue
            ctype, encoding = mimetypes.guess_type(str(path))
            if ctype is None or encoding is not None:
                maintype, subtype = "application", "octet-stream"
            else:
                maintype, subtype = ctype.split("/", 1)
            msg.add_attachment(path.read_bytes(), maintype=maintype, subtype=subtype, filename=path.name)
            attached.append(path.name)
        recipients = [to] + cc_list
        with smtplib.SMTP(box["smtp_host"], box["smtp_port"], timeout=30) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(from_email, box["password"])
            smtp.send_message(msg, to_addrs=recipients)
        store_message({
            "message_key": message_key,
            "channel": "email",
            "direction": "out",
            "message_id": msg.get("Message-ID", ""),
            "conversation_id": conversation_id or f"email:{reply_to_id or to}",
            "sender_id": from_email,
            "recipient_id": to,
            "subject": subject,
            "body": plain,
            "attachments": attachments,
            "metadata": {"cc": cc_list, "reply_to_id": reply_to_id, "mailbox": mailbox},
            "status": "sent",
            "created_at": utc_now(),
        })
        with db_conn() as conn:
            conn.execute(
                "INSERT INTO email_log(direction,from_addr,to_addr,subject,body_preview,sent_at) VALUES(?,?,?,?,?,?)",
                ("OUT", from_email, to, subject, plain[:300], utc_now()),
            )
        log.info("Email odoslaný (%s): To=%s | %s", mailbox, to, subject)
        return {"ok": True, "attachments_sent": attached, "attachments_missing": missing, "cc_sent": cc_list, "message_key": message_key}
    except smtplib.SMTPAuthenticationError:
        return {"ok": False, "error": "SMTP autentifikácia zlyhala. Použi App Password."}
    except Exception as exc:
        log.exception("SMTP send chyba")
        return {"ok": False, "error": str(exc)}


def generate_offer_number() -> str:
    year = datetime.now().year
    with db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM offers WHERE offer_number LIKE ?", (f"PON-{year}-%",)).fetchone()
    return f"PON-{year}-{int(row['c']) + 1:04d}"


def save_offer_db(offer: Dict[str, Any]) -> Dict[str, Any]:
    offer_number = str(offer.get("offer_number") or generate_offer_number()).upper()
    try:
        with db_conn() as conn:
            conn.execute(
                """
                INSERT INTO offers
                (offer_number,customer_email,customer_name,company,service_type,status,
                 total_monthly_eur,total_setup_eur,offer_folder,email_thread_id,notes,
                 created_at,updated_at,supervisor_approved,client_accepted)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(offer_number) DO UPDATE SET
                  customer_email=excluded.customer_email,
                  customer_name=excluded.customer_name,
                  company=excluded.company,
                  service_type=excluded.service_type,
                  status=excluded.status,
                  total_monthly_eur=excluded.total_monthly_eur,
                  total_setup_eur=excluded.total_setup_eur,
                  offer_folder=excluded.offer_folder,
                  email_thread_id=excluded.email_thread_id,
                  notes=excluded.notes,
                  updated_at=excluded.updated_at,
                  supervisor_approved=excluded.supervisor_approved,
                  client_accepted=excluded.client_accepted
                """,
                (
                    offer_number,
                    offer.get("customer_email", ""), offer.get("customer_name", ""), offer.get("company", ""),
                    offer.get("service_type", ""), offer.get("status", "draft"),
                    offer.get("total_monthly_eur", 0), offer.get("total_setup_eur", 0),
                    offer.get("offer_folder", ""), offer.get("email_thread_id", ""), offer.get("notes", ""),
                    offer.get("created_at", utc_now()), utc_now(),
                    int(bool(offer.get("supervisor_approved", False))), int(bool(offer.get("client_accepted", False))),
                ),
            )
        return {"ok": True, "offer_number": offer_number}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def list_offers(status: str = "", limit: int = 100) -> List[Dict[str, Any]]:
    with db_conn() as conn:
        if status:
            rows = conn.execute("SELECT * FROM offers WHERE status=? ORDER BY created_at DESC LIMIT ?", (status, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM offers ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    return [dict(row) for row in rows]


def save_contract_db(contract: Dict[str, Any]) -> Dict[str, Any]:
    with db_conn() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM contracts").fetchone()
        number = contract.get("contract_number") or f"ZML-{datetime.now().year}-{int(row['c']) + 1:04d}"
        conn.execute(
            """
            INSERT INTO contracts
            (contract_number,offer_id,customer_email,customer_name,company,ico,ic_dph,address,
             service_description,monthly_fee_eur,setup_fee_eur,contract_duration_months,
             signed_date,start_date,status,contract_file,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(contract_number) DO UPDATE SET
              status=excluded.status, contract_file=excluded.contract_file, updated_at=excluded.updated_at
            """,
            (
                number, contract.get("offer_id"), contract.get("customer_email", ""), contract.get("customer_name", ""),
                contract.get("company", ""), contract.get("ico", ""), contract.get("ic_dph", ""), contract.get("address", ""),
                contract.get("service_description", ""), contract.get("monthly_fee_eur", 0), contract.get("setup_fee_eur", 0),
                contract.get("contract_duration_months", 12), contract.get("signed_date", ""),
                contract.get("start_date", datetime.now().strftime("%Y-%m-%d")), contract.get("status", "draft"),
                contract.get("contract_file", ""), contract.get("created_at", utc_now()), utc_now(),
            ),
        )
    return {"ok": True, "contract_number": number}


def register_agent(data: Dict[str, Any]) -> Dict[str, Any]:
    agent_id = str(data.get("agent_id") or data.get("name") or uuid.uuid4()).strip()
    if not agent_id:
        return {"ok": False, "error": "Chýba agent_id"}
    now = utc_now()
    with db_conn() as conn:
        conn.execute(
            """
            INSERT INTO agents(agent_id,name,version,capabilities_json,metadata_json,status,registered_at,last_heartbeat)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(agent_id) DO UPDATE SET
              name=excluded.name, version=excluded.version,
              capabilities_json=excluded.capabilities_json, metadata_json=excluded.metadata_json,
              status='online', last_heartbeat=excluded.last_heartbeat
            """,
            (agent_id, data.get("name", agent_id), data.get("version", ""), json_text(data.get("capabilities", [])),
             json_text(data.get("metadata", {})), "online", now, now),
        )
    return {"ok": True, "agent_id": agent_id, "registered_at": now}


def heartbeat_agent(agent_id: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    now = utc_now()
    with db_conn() as conn:
        result = conn.execute(
            "UPDATE agents SET status='online', last_heartbeat=?, metadata_json=COALESCE(?,metadata_json) WHERE agent_id=?",
            (now, json_text(metadata) if metadata is not None else None, agent_id),
        )
        if result.rowcount == 0:
            return {"ok": False, "error": f"Agent {agent_id} nie je registrovaný"}
    return {"ok": True, "agent_id": agent_id, "last_heartbeat": now}


def list_agents() -> List[Dict[str, Any]]:
    with db_conn() as conn:
        rows = conn.execute("SELECT * FROM agents ORDER BY name").fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["capabilities"] = json_load(item.pop("capabilities_json", "[]"), [])
        item["metadata"] = json_load(item.pop("metadata_json", "{}"), {})
        result.append(item)
    return result


def publish_event(data: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(data.get("event_id") or uuid.uuid4())
    idem = str(data.get("idempotency_key") or f"event:{event_id}")
    now = utc_now()
    try:
        with db_conn() as conn:
            conn.execute(
                """
                INSERT INTO events(event_id,event_type,source_agent,target_agent,conversation_id,correlation_id,
                                   payload_json,priority,status,idempotency_key,created_at,available_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (event_id, data["event_type"], data.get("source_agent", ""), data.get("target_agent", "*"),
                 data.get("conversation_id", ""), data.get("correlation_id", ""), json_text(data.get("payload", {})),
                 int(data.get("priority", 5)), "pending", idem, now, data.get("available_at", now)),
            )
        return {"ok": True, "event_id": event_id, "status": "pending"}
    except sqlite3.IntegrityError:
        with db_conn() as conn:
            row = conn.execute("SELECT event_id,status FROM events WHERE idempotency_key=?", (idem,)).fetchone()
        return {"ok": True, "duplicate": True, "event_id": row["event_id"] if row else event_id, "status": row["status"] if row else "unknown"}


def get_events(agent_id: str, limit: int = 20, claim: bool = True) -> List[Dict[str, Any]]:
    now = utc_now()
    with db_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM events
            WHERE status='pending' AND available_at<=? AND (target_agent=? OR target_agent='*' OR target_agent='')
            ORDER BY priority ASC, created_at ASC LIMIT ?
            """,
            (now, agent_id, limit),
        ).fetchall()
        event_ids = [row["event_id"] for row in rows]
        if claim and event_ids:
            conn.executemany("UPDATE events SET status='claimed', claimed_at=? WHERE event_id=? AND status='pending'", [(now, eid) for eid in event_ids])
    result = []
    for row in rows:
        item = dict(row)
        item["payload"] = json_load(item.pop("payload_json", "{}"), {})
        if claim:
            item["status"] = "claimed"
        result.append(item)
    return result


def ack_event(event_id: str, ok: bool = True, error: str = "") -> Dict[str, Any]:
    status = "acknowledged" if ok else "failed"
    with db_conn() as conn:
        result = conn.execute(
            "UPDATE events SET status=?, acknowledged_at=?, error=? WHERE event_id=?",
            (status, utc_now(), error, event_id),
        )
    return {"ok": result.rowcount > 0, "event_id": event_id, "status": status}


def record_access_event(data: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(data.get("event_id") or uuid.uuid4())
    with db_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO access_events(event_id,source,camera_id,person_ref,authorized,occurred_at,metadata_json) VALUES(?,?,?,?,?,?,?)",
            (event_id, data.get("source", "access-control"), data.get("camera_id", ""), data.get("person_ref", ""),
             int(bool(data.get("authorized", False))), data.get("occurred_at", utc_now()), json_text(data.get("metadata", {}))),
        )
    return {"ok": True, "event_id": event_id}


def record_watchdog_event(data: Dict[str, Any]) -> Dict[str, Any]:
    event_id = str(data.get("event_id") or uuid.uuid4())
    occurred_at = data.get("occurred_at", utc_now())
    with db_conn() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO watchdog_events
            (event_id,camera_id,event_type,authorization_status,person_ref,snapshot_path,confidence,metadata_json,occurred_at,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (event_id, data.get("camera_id", ""), data.get("event_type", "person_detected"),
             data.get("authorization_status", "unverified"), data.get("person_ref", ""), data.get("snapshot_path", ""),
             data.get("confidence"), json_text(data.get("metadata", {})), occurred_at, utc_now()),
        )
    publish_event({
        "event_type": "watchdog.person_detected",
        "source_agent": data.get("source_agent", "watchdog"),
        "target_agent": data.get("target_agent", "notification-assistant"),
        "correlation_id": event_id,
        "priority": 1 if data.get("authorization_status") in {"unauthorized", "unverified"} else 5,
        "payload": {**data, "event_id": event_id},
        "idempotency_key": f"watchdog:{event_id}",
    })
    return {"ok": True, "event_id": event_id}


def list_watchdog_events(limit: int = 100, authorization_status: str = "") -> List[Dict[str, Any]]:
    with db_conn() as conn:
        if authorization_status:
            rows = conn.execute("SELECT * FROM watchdog_events WHERE authorization_status=? ORDER BY occurred_at DESC LIMIT ?", (authorization_status, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM watchdog_events ORDER BY occurred_at DESC LIMIT ?", (limit,)).fetchall()
    result = []
    for row in rows:
        item = dict(row)
        item["metadata"] = json_load(item.pop("metadata_json", "{}"), {})
        result.append(item)
    return result


CAPABILITIES = {
    "server": SERVER_NAME,
    "version": "2.0.0",
    "protocols": ["REST", "MCP-SSE"],
    "domains": ["agents", "events", "messages", "email", "offers", "contracts", "watchdog"],
    "backward_compatible_endpoints": ["/health", "/status", "/emails", "/offers", "/tool/send_email", "/tool/save_offer", "/tool/save_contract", "/tool/mark_email_processed", "/sse"],
}


mcp = Server(SERVER_NAME)


@mcp.list_tools()
async def list_tools():
    import mcp.types as types
    def tool(name: str, description: str, properties: Dict[str, Any], required: Optional[List[str]] = None):
        schema: Dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        return types.Tool(name=name, description=description, inputSchema=schema)
    return [
        tool("get_capabilities", "Vráti schopnosti centrálneho Peter Gateway servera", {}),
        tool("register_agent", "Registruje asistenta na centrálnom serveri", {
            "agent_id": {"type": "string"}, "name": {"type": "string"}, "version": {"type": "string"},
            "capabilities": {"type": "array", "items": {"type": "string"}}, "metadata": {"type": "object"}}, ["agent_id", "name"]),
        tool("heartbeat_agent", "Aktualizuje heartbeat asistenta", {"agent_id": {"type": "string"}, "metadata": {"type": "object"}}, ["agent_id"]),
        tool("list_agents", "Zoznam registrovaných asistentov", {}),
        tool("publish_event", "Publikuje udalosť pre iného asistenta", {
            "event_type": {"type": "string"}, "source_agent": {"type": "string"}, "target_agent": {"type": "string"},
            "conversation_id": {"type": "string"}, "correlation_id": {"type": "string"}, "payload": {"type": "object"},
            "priority": {"type": "integer"}, "idempotency_key": {"type": "string"}}, ["event_type"]),
        tool("get_events", "Načíta a voliteľne rezervuje udalosti pre asistenta", {
            "agent_id": {"type": "string"}, "limit": {"type": "integer", "default": 20}, "claim": {"type": "boolean", "default": True}}, ["agent_id"]),
        tool("ack_event", "Potvrdí úspešné alebo neúspešné spracovanie udalosti", {
            "event_id": {"type": "string"}, "ok": {"type": "boolean", "default": True}, "error": {"type": "string"}}, ["event_id"]),
        tool("check_new_emails", "Skontroluje nové emaily vo všetkých nakonfigurovaných schránkach (sales, flm, ...)", {"max_count": {"type": "integer", "default": 10}}),
        tool("send_email", "Odošle email s prílohami z danej schránky (mailbox: sales|flm, predvolené sales)", {
            "to": {"type": "string"}, "subject": {"type": "string"}, "body_html": {"type": "string"},
            "body_text": {"type": "string"}, "reply_to_id": {"type": "string"}, "conversation_id": {"type": "string"},
            "idempotency_key": {"type": "string"}, "cc": {"type": "array", "items": {"type": "string"}},
            "attachments": {"type": "array", "items": {"type": "string"}}, "mailbox": {"type": "string", "default": "sales"}}, ["to", "subject", "body_html"]),
        tool("mark_email_processed", "Označí email ako spracovaný v danej schránke", {
            "imap_id": {"type": "string"}, "mailbox": {"type": "string", "default": "sales"}}, ["imap_id"]),
        tool("save_offer", "Uloží alebo aktualizuje ponuku", {"offer": {"type": "object"}}, ["offer"]),
        tool("list_offers", "Vráti ponuky", {"status": {"type": "string"}, "limit": {"type": "integer", "default": 100}}),
        tool("save_contract", "Uloží alebo aktualizuje zmluvu", {"contract": {"type": "object"}}, ["contract"]),
        tool("record_access_event", "Zapíše udalosť z prístupového systému", {"event": {"type": "object"}}, ["event"]),
        tool("record_watchdog_event", "Zapíše detekciu osoby z kamery a publikuje udalosť", {"event": {"type": "object"}}, ["event"]),
        tool("list_watchdog_events", "Vráti watchdog udalosti", {"limit": {"type": "integer", "default": 100}, "authorization_status": {"type": "string"}}),
    ]


@mcp.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]):
    import mcp.types as types
    if name == "get_capabilities": result = CAPABILITIES
    elif name == "register_agent": result = register_agent(arguments)
    elif name == "heartbeat_agent": result = heartbeat_agent(arguments["agent_id"], arguments.get("metadata"))
    elif name == "list_agents": result = list_agents()
    elif name == "publish_event": result = publish_event(arguments)
    elif name == "get_events": result = get_events(arguments["agent_id"], int(arguments.get("limit", 20)), bool(arguments.get("claim", True)))
    elif name == "ack_event": result = ack_event(arguments["event_id"], bool(arguments.get("ok", True)), arguments.get("error", ""))
    elif name == "check_new_emails": result = fetch_new_emails(int(arguments.get("max_count", 10)))
    elif name == "send_email": result = send_email_smtp(**arguments)
    elif name == "mark_email_processed": result = {"ok": mark_email_read(arguments["imap_id"], arguments.get("mailbox", "sales"))}
    elif name == "save_offer": result = save_offer_db(arguments.get("offer", arguments))
    elif name == "list_offers": result = list_offers(arguments.get("status", ""), int(arguments.get("limit", 100)))
    elif name == "save_contract": result = save_contract_db(arguments.get("contract", arguments))
    elif name == "record_access_event": result = record_access_event(arguments.get("event", arguments))
    elif name == "record_watchdog_event": result = record_watchdog_event(arguments.get("event", arguments))
    elif name == "list_watchdog_events": result = list_watchdog_events(int(arguments.get("limit", 100)), arguments.get("authorization_status", ""))
    else: result = {"ok": False, "error": f"Neznámy nástroj: {name}"}
    return [types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False, indent=2))]


class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not API_KEY or request.url.path in {"/health", "/status"} or request.url.path.startswith("/sse") or request.url.path.startswith("/messages"):
            return await call_next(request)
        supplied = request.headers.get("x-api-key", "")
        auth = request.headers.get("authorization", "")
        if supplied != API_KEY and auth != f"Bearer {API_KEY}":
            return JSONResponse({"ok": False, "error": "Unauthorized"}, status_code=401)
        return await call_next(request)


async def body_json(request: Request) -> Dict[str, Any]:
    try:
        return await request.json()
    except Exception:
        return {}


async def route_health(_: Request):
    return JSONResponse({"status": "ok", "server": SERVER_NAME, "version": "2.0.0", "port": PORT, "time": utc_now()})


async def route_status(_: Request):
    with db_conn() as conn:
        counts = {
            "agents": conn.execute("SELECT COUNT(*) c FROM agents").fetchone()["c"],
            "pending_events": conn.execute("SELECT COUNT(*) c FROM events WHERE status='pending'").fetchone()["c"],
            "offers": conn.execute("SELECT COUNT(*) c FROM offers").fetchone()["c"],
            "contracts": conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"],
            "watchdog_events": conn.execute("SELECT COUNT(*) c FROM watchdog_events").fetchone()["c"],
        }
    return JSONResponse({**CAPABILITIES, "email": SALES_EMAIL,
                          "mailboxes": [b["id"] for b in MAILBOXES if b["password"]],
                          "db": str(DB_PATH), "offers_dir": str(OFFERS_DIR), "counts": counts})


async def route_capabilities(_: Request): return JSONResponse(CAPABILITIES)
async def route_emails(request: Request): return JSONResponse(fetch_new_emails(int(request.query_params.get("max_count", "5"))))
async def route_offers(request: Request): return JSONResponse(list_offers(request.query_params.get("status", ""), int(request.query_params.get("limit", "100"))))
async def route_agents(_: Request): return JSONResponse(list_agents())
async def route_register_agent(request: Request): return JSONResponse(register_agent(await body_json(request)))
async def route_heartbeat(request: Request):
    data = await body_json(request); return JSONResponse(heartbeat_agent(data.get("agent_id", ""), data.get("metadata")))
async def route_publish_event(request: Request): return JSONResponse(publish_event(await body_json(request)))
async def route_get_events(request: Request):
    return JSONResponse(get_events(request.query_params.get("agent_id", ""), int(request.query_params.get("limit", "20")), request.query_params.get("claim", "true").lower() == "true"))
async def route_ack_event(request: Request):
    data = await body_json(request); return JSONResponse(ack_event(data.get("event_id", ""), bool(data.get("ok", True)), data.get("error", "")))
async def route_watchdog_events(request: Request):
    return JSONResponse(list_watchdog_events(int(request.query_params.get("limit", "100")), request.query_params.get("authorization_status", "")))
async def route_record_watchdog(request: Request): return JSONResponse(record_watchdog_event(await body_json(request)))
async def route_record_access(request: Request): return JSONResponse(record_access_event(await body_json(request)))
async def route_tool_send_email(request: Request):
    result = send_email_smtp(**(await body_json(request))); return JSONResponse(result, status_code=200 if result.get("ok") else 400)
async def route_tool_save_offer(request: Request):
    result = save_offer_db(await body_json(request)); return JSONResponse(result, status_code=200 if result.get("ok") else 400)
async def route_tool_save_contract(request: Request):
    result = save_contract_db(await body_json(request)); return JSONResponse(result, status_code=200 if result.get("ok") else 400)
async def route_tool_mark_email_processed(request: Request):
    data = await body_json(request)
    result = {"ok": mark_email_read(data.get("imap_id", ""), data.get("mailbox", "sales"))}
    return JSONResponse(result, status_code=200 if result["ok"] else 400)


sse = SseServerTransport("/messages")


async def route_sse(request: Request):
    async with sse.connect_sse(request.scope, request.receive, request._send) as streams:
        await mcp.run(
            streams[0], streams[1],
            InitializationOptions(
                server_name=SERVER_NAME,
                server_version="2.0.0",
                capabilities=mcp.get_capabilities(notification_options=NotificationOptions(), experimental_capabilities={}),
            ),
        )


routes = [
    Route("/health", route_health), Route("/status", route_status), Route("/capabilities", route_capabilities),
    Route("/emails", route_emails), Route("/offers", route_offers),
    Route("/tool/send_email", route_tool_send_email, methods=["POST"]),
    Route("/tool/save_offer", route_tool_save_offer, methods=["POST"]),
    Route("/tool/save_contract", route_tool_save_contract, methods=["POST"]),
    Route("/tool/mark_email_processed", route_tool_mark_email_processed, methods=["POST"]),
    Route("/api/v1/agents", route_agents), Route("/api/v1/agents/register", route_register_agent, methods=["POST"]),
    Route("/api/v1/agents/heartbeat", route_heartbeat, methods=["POST"]),
    Route("/api/v1/events", route_get_events), Route("/api/v1/events/publish", route_publish_event, methods=["POST"]),
    Route("/api/v1/events/ack", route_ack_event, methods=["POST"]),
    Route("/api/v1/watchdog/events", route_watchdog_events), Route("/api/v1/watchdog/events", route_record_watchdog, methods=["POST"]),
    Route("/api/v1/access/events", route_record_access, methods=["POST"]),
    Route("/sse", route_sse), Mount("/messages", app=sse.handle_post_message),
]

app = Starlette(routes=routes)
app.add_middleware(ApiKeyMiddleware)


def main() -> None:
    print("=" * 72)
    print("  Peter Gateway Server v2.0")
    print(f"  REST/MCP : http://127.0.0.1:{PORT}")
    print(f"  Email    : {SALES_EMAIL}")
    print(f"  DB       : {DB_PATH}")
    print(f"  API auth : {'enabled' if API_KEY else 'disabled (local development)'}")
    print("=" * 72)
    uvicorn.run(app, host=HOST, port=PORT, log_level=os.getenv("UVICORN_LOG_LEVEL", "warning"))


if __name__ == "__main__":
    main()
