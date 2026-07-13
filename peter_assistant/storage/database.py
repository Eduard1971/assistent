from __future__ import annotations
import json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

class Database:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init(self) -> None:
        with self.connect() as conn:
            conn.executescript("""
            CREATE TABLE IF NOT EXISTS processed_messages(
              channel TEXT NOT NULL,
              message_id TEXT NOT NULL,
              processed_at TEXT NOT NULL,
              PRIMARY KEY(channel,message_id)
            );
            CREATE TABLE IF NOT EXISTS conversations(
              conversation_id TEXT PRIMARY KEY,
              customer_id TEXT,
              channel TEXT,
              state TEXT NOT NULL DEFAULT 'new',
              service_type TEXT,
              data_json TEXT NOT NULL DEFAULT '{}',
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS messages(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              conversation_id TEXT NOT NULL,
              direction TEXT NOT NULL,
              channel TEXT NOT NULL,
              message_id TEXT,
              subject TEXT,
              body TEXT,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS offers(
              offer_number TEXT PRIMARY KEY,
              conversation_id TEXT NOT NULL,
              customer_id TEXT NOT NULL,
              state TEXT NOT NULL,
              service_type TEXT,
              data_json TEXT NOT NULL DEFAULT '{}',
              documents_json TEXT NOT NULL DEFAULT '[]',
              updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS learning_queue(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              question TEXT NOT NULL,
              context_json TEXT NOT NULL DEFAULT '{}',
              state TEXT NOT NULL DEFAULT 'pending_review',
              answer TEXT,
              created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS watchdog_events(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              camera_id TEXT NOT NULL,
              event_type TEXT NOT NULL,
              authorized INTEGER NOT NULL,
              evidence_path TEXT,
              data_json TEXT NOT NULL DEFAULT '{}',
              created_at TEXT NOT NULL
            );
            """)

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def is_processed(self, channel: str, message_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT 1 FROM processed_messages WHERE channel=? AND message_id=?", (channel, message_id)).fetchone()
            return bool(row)

    def mark_processed(self, channel: str, message_id: str) -> None:
        with self.connect() as conn:
            conn.execute("INSERT OR IGNORE INTO processed_messages VALUES (?,?,?)", (channel, message_id, self.now()))

    def get_conversation(self, conversation_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM conversations WHERE conversation_id=?", (conversation_id,)).fetchone()
        if not row:
            return {}
        data = dict(row)
        data["data"] = json.loads(data.pop("data_json") or "{}")
        return data

    def upsert_conversation(self, conversation_id: str, customer_id: str, channel: str, state: str, service_type: str = "", data: dict[str, Any] | None = None) -> None:
        payload = json.dumps(data or {}, ensure_ascii=False)
        with self.connect() as conn:
            conn.execute("""
            INSERT INTO conversations(conversation_id,customer_id,channel,state,service_type,data_json,updated_at)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(conversation_id) DO UPDATE SET
              customer_id=excluded.customer_id, channel=excluded.channel, state=excluded.state,
              service_type=excluded.service_type, data_json=excluded.data_json, updated_at=excluded.updated_at
            """, (conversation_id, customer_id, channel, state, service_type, payload, self.now()))

    def add_message(self, conversation_id: str, direction: str, channel: str, message_id: str, subject: str, body: str) -> None:
        with self.connect() as conn:
            conn.execute("INSERT INTO messages(conversation_id,direction,channel,message_id,subject,body,created_at) VALUES(?,?,?,?,?,?,?)", (conversation_id,direction,channel,message_id,subject,body,self.now()))

    def add_learning_item(self, question: str, context: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute("INSERT INTO learning_queue(question,context_json,created_at) VALUES(?,?,?)", (question,json.dumps(context,ensure_ascii=False),self.now()))

    def add_watchdog_event(self, camera_id: str, event_type: str, authorized: bool, evidence_path: str | None, data: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute("INSERT INTO watchdog_events(camera_id,event_type,authorized,evidence_path,data_json,created_at) VALUES(?,?,?,?,?,?)", (camera_id,event_type,1 if authorized else 0,evidence_path,json.dumps(data,ensure_ascii=False),self.now()))
