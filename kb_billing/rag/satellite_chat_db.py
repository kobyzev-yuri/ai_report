#!/usr/bin/env python3
"""
Хранение истории диалогов оператора и спутникового ассистента в SQLite.
Используется для сохранения контекста между перезагрузками страницы и сеансами.
"""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

# БД рядом с модулем (в deploy копируется вместе с кодом)
DB_PATH = Path(__file__).resolve().parent / "satellite_chat.db"


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    """Создать таблицу сообщений, если её нет."""
    conn = _get_conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS satellite_chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                persona TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_satellite_chat_username_persona "
            "ON satellite_chat_messages(username, persona)"
        )
        conn.commit()
    finally:
        conn.close()


def get_history(username: str, persona: str, limit: int = 50) -> List[dict]:
    """
    Загрузить историю сообщений для пользователя и персоны (sat_eng / sat_sub).
    Возвращает список {"role": "user"|"assistant", "content": "..."} в порядке времени.
    """
    if not username or not persona:
        return []
    init_db()
    conn = _get_conn()
    try:
        cur = conn.execute(
            """
            SELECT role, content FROM satellite_chat_messages
            WHERE username = ? AND persona = ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (username.strip(), persona.strip(), limit),
        )
        return [{"role": row["role"], "content": row["content"]} for row in cur.fetchall()]
    finally:
        conn.close()


def append_message(username: str, persona: str, role: str, content: str) -> None:
    """Добавить одно сообщение в историю."""
    if not username or not persona or not content:
        return
    init_db()
    conn = _get_conn()
    try:
        conn.execute(
            """
            INSERT INTO satellite_chat_messages (username, persona, role, content, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (username.strip(), persona.strip(), role.strip(), content.strip(), datetime.utcnow().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


def clear_history(username: str, persona: str) -> None:
    """Удалить всю историю диалога для данного пользователя и персоны."""
    if not username or not persona:
        return
    init_db()
    conn = _get_conn()
    try:
        conn.execute(
            "DELETE FROM satellite_chat_messages WHERE username = ? AND persona = ?",
            (username.strip(), persona.strip()),
        )
        conn.commit()
    finally:
        conn.close()
