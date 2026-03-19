"""
persistence.py — Async SQLite persistence layer using aiosqlite.

Manages schema creation, idempotent inserts (UNIQUE on telegram_id),
and retry-with-backoff for writes.
"""
from __future__ import annotations

import asyncio
import functools
import logging
from typing import Optional

import aiosqlite

from config import PARSED_DB
from models import ParsedMessage

logger = logging.getLogger(__name__)

# ── Schema ───────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS mensajes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    telegram_id     INTEGER UNIQUE,
    estacion        TEXT,
    tipo_mensaje    TEXT,
    canal           TEXT,
    timestamp       DATETIME,
    tono            BOOLEAN
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_station_time ON mensajes (estacion, timestamp);",
    "CREATE INDEX IF NOT EXISTS idx_tono_search  ON mensajes (tono);",
    "CREATE INDEX IF NOT EXISTS idx_timestamp    ON mensajes (timestamp);",
]

_INSERT = """
INSERT OR IGNORE INTO mensajes
    (telegram_id, estacion, tipo_mensaje, canal, timestamp, tono)
VALUES (?, ?, ?, ?, ?, ?);
"""


# ── Retry decorator ─────────────────────────────────────────

def retry_on_locked(max_retries: int = 5, base_delay: float = 0.1):
    """Decorator: retry with exponential backoff when DB is locked."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except aiosqlite.OperationalError as exc:
                    if "locked" in str(exc).lower() and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            "DB locked (attempt %d/%d), retrying in %.2fs",
                            attempt + 1, max_retries, delay,
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise
        return wrapper
    return decorator


# ── Public API ───────────────────────────────────────────────

async def init_db() -> None:
    """Create the mensajes table and indexes if they don't exist."""
    async with aiosqlite.connect(str(PARSED_DB)) as db:
        await db.execute(_CREATE_TABLE)
        for idx_sql in _CREATE_INDEXES:
            await db.execute(idx_sql)
        await db.commit()
    logger.info("Database initialized: %s", PARSED_DB)


@retry_on_locked()
async def insert_message(msg: ParsedMessage) -> bool:
    """
    Insert a parsed message. Idempotent via UNIQUE(telegram_id).

    Returns True if a new row was inserted, False if it already existed.
    """
    async with aiosqlite.connect(str(PARSED_DB)) as db:
        cursor = await db.execute(
            _INSERT,
            (
                msg.telegram_id,
                msg.estacion,
                msg.tipo_mensaje.value,
                msg.canal,
                msg.timestamp.isoformat(),
                msg.tono,
            ),
        )
        await db.commit()
        return cursor.rowcount > 0


@retry_on_locked()
async def message_exists(telegram_id: int) -> bool:
    """Check whether a message with the given telegram_id is already stored."""
    async with aiosqlite.connect(str(PARSED_DB)) as db:
        async with db.execute(
            "SELECT 1 FROM mensajes WHERE telegram_id = ?",
            (telegram_id,),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None


@retry_on_locked()
async def get_messages_by_station_date(
    station_name: str, date_str: str
) -> list[dict]:
    """
    Fetch all messages for a station on a given date (YYYY-MM-DD).
    Returns dicts with keys matching column names.
    """
    async with aiosqlite.connect(str(PARSED_DB)) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM mensajes "
            "WHERE estacion = ? AND date(timestamp) = ? "
            "ORDER BY timestamp",
            (station_name, date_str),
            ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


@retry_on_locked()
async def has_open_and_close(
    station_name: str, start: datetime, end: datetime
) -> bool:
    """
    Check if the database already contains at least one OPEN and one CLOSE
    message for the given station within the [start, end] range.
    """
    async with aiosqlite.connect(str(PARSED_DB)) as db:
        async with db.execute(
            "SELECT COUNT(DISTINCT tipo_mensaje) FROM mensajes "
            "WHERE estacion = ? AND timestamp BETWEEN ? AND ? "
            "AND tipo_mensaje IN ('OPEN', 'CLOSE') AND tono = 1",
            (station_name, start.isoformat(), end.isoformat()),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None and row[0] == 2
