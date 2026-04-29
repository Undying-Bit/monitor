"""
persistence.py - Async SQLite persistence layer.

Manages schema creation, WAL settings, idempotent inserts, and retries.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiosqlite
from contextlib import asynccontextmanager

from config import PARSED_DB, STATE_PAIR_WINDOW_SECONDS
from models import NormalizedEvent, RawEvent

logger = logging.getLogger(__name__)

_PAIRABLE_STATE_TYPES = frozenset({"OPEN", "CLOSE"})
_STATE_PAIR_EVENT_ID_KEY = "state_pair_event_id"
_STATE_PAIR_EVENT_TYPE_KEY = "state_pair_event_type"
_STATE_PAIR_DELTA_SECONDS_KEY = "state_pair_delta_seconds"
_STATE_PAIR_DIRECTION_KEY = "state_pair_direction"
_STATE_PAIR_WINDOW_SECONDS_KEY = "state_pair_window_seconds"
_STATE_PAIR_PROMOTED_KEY = "state_pair_promoted"
_STATE_PAIR_ORIGINAL_REPORT_SLOT_KEY = "state_pair_original_report_slot"
_STATE_PAIR_ORIGINAL_TONE_KEY = "state_pair_original_tone"
_STATE_PAIR_ORIGINAL_IS_VALID_REPORT_KEY = "state_pair_original_is_valid_report"

# ── Schema ──────────────────────────────────────────────────────────────────
_CREATE_RAW_EVENTS = """
CREATE TABLE IF NOT EXISTS raw_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    source_event_id TEXT,
    received_at TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    raw_hash TEXT NOT NULL UNIQUE,
    transport_meta_json TEXT,
    parse_status TEXT NOT NULL DEFAULT 'pending',
    parse_error TEXT
);
"""

_CREATE_NORMALIZED_EVENTS = """
CREATE TABLE IF NOT EXISTS normalized_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_event_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    station_id INTEGER,
    station_name TEXT,
    station_code TEXT,
    report_date TEXT,
    report_slot TEXT,
    event_type TEXT NOT NULL,
    event_class TEXT NOT NULL,
    channel TEXT,
    tone INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    is_valid_report INTEGER NOT NULL DEFAULT 0,
    event_time_utc TEXT,
    event_time_local TEXT NOT NULL,
    confidence_score INTEGER NOT NULL DEFAULT 0,
    parser_version TEXT NOT NULL,
    transmitter_code TEXT,
    phone_number TEXT,
    payload_json TEXT,
    FOREIGN KEY (raw_event_id) REFERENCES raw_events(id)
);
"""

_CREATE_PRUEBAS = """
CREATE TABLE IF NOT EXISTS pruebas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_event_id INTEGER NOT NULL UNIQUE,
    source TEXT NOT NULL,
    station_id INTEGER,
    station_name TEXT,
    station_code TEXT,
    report_date TEXT,
    report_slot TEXT,
    event_type TEXT,
    event_class TEXT,
    tone INTEGER,
    event_time_local TEXT NOT NULL,
    event_time_utc TEXT,
    confidence_score INTEGER,
    parser_version TEXT,
    transmitter_code TEXT,
    phone_number TEXT,
    channel TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (normalized_event_id) REFERENCES normalized_events(id)
);
"""

_CREATE_ALERTAS = """
CREATE TABLE IF NOT EXISTS alertas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    normalized_event_id INTEGER NOT NULL UNIQUE,
    source TEXT NOT NULL,
    station_id INTEGER,
    station_name TEXT,
    station_code TEXT,
    report_date TEXT,
    report_slot TEXT,
    event_type TEXT,
    event_class TEXT,
    tone INTEGER,
    event_time_local TEXT NOT NULL,
    event_time_utc TEXT,
    confidence_score INTEGER,
    parser_version TEXT,
    transmitter_code TEXT,
    phone_number TEXT,
    channel TEXT,
    payload_json TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (normalized_event_id) REFERENCES normalized_events(id)
);
"""

_CREATE_RESOLVED_SLOTS = """
CREATE TABLE IF NOT EXISTS resolved_report_slots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    station_id INTEGER NOT NULL,
    station_name TEXT NOT NULL,
    report_date TEXT NOT NULL,
    report_slot TEXT NOT NULL,
    effective_event_id INTEGER,
    effective_source TEXT,
    effective_event_type TEXT,
    effective_confidence INTEGER DEFAULT 0,
    first_seen_at TEXT,
    last_updated_at TEXT NOT NULL,
    UNIQUE (station_id, report_date, report_slot)
);
"""

_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_raw_source_event ON raw_events (source, source_event_id);",
    (
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_telegram_event "
        "ON raw_events (source, source_event_id) "
        "WHERE source = 'telegram' AND source_event_id IS NOT NULL;"
    ),
    "CREATE INDEX IF NOT EXISTS idx_raw_status ON raw_events (parse_status);",
    "CREATE INDEX IF NOT EXISTS idx_norm_raw_event ON normalized_events (raw_event_id);",
    "CREATE INDEX IF NOT EXISTS idx_norm_station_slot ON normalized_events (station_id, report_date, report_slot);",
    (
        "CREATE INDEX IF NOT EXISTS idx_norm_state_pair_lookup "
        "ON normalized_events (source, event_class, station_id, report_date, event_type, event_time_local);"
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_norm_resolve_slot_lookup "
        "ON normalized_events (station_id, report_date, report_slot, is_valid_report, priority, confidence_score, event_time_local);"
    ),
    "CREATE INDEX IF NOT EXISTS idx_norm_time ON normalized_events (event_time_local);",
    "CREATE INDEX IF NOT EXISTS idx_pruebas_station_time ON pruebas (station_name, event_time_local);",
    "CREATE INDEX IF NOT EXISTS idx_alertas_station_time ON alertas (station_name, event_time_local);",
]

_CREATE_VIEWS = [
    "CREATE VIEW IF NOT EXISTS incoming_messages AS SELECT * FROM raw_events;",
    "CREATE VIEW IF NOT EXISTS station_events AS SELECT * FROM normalized_events;",
]

_DROP_TRIGGERS = [
    "DROP TRIGGER IF EXISTS trg_pruebas_insert;",
    "DROP TRIGGER IF EXISTS trg_alertas_insert;",
    "DROP TRIGGER IF EXISTS trg_pruebas_delete;",
]

_FINALIZED_RAW_STATUSES = frozenset({"complete", "error", "partial"})

_RAW_EVENTS_EXPECTED_TABLE_INFO = (
    ("id", "INTEGER", 0, None, 1),
    ("source", "TEXT", 1, None, 0),
    ("source_event_id", "TEXT", 0, None, 0),
    ("received_at", "TEXT", 1, None, 0),
    ("raw_payload", "TEXT", 1, None, 0),
    ("raw_hash", "TEXT", 1, None, 0),
    ("transport_meta_json", "TEXT", 0, None, 0),
    ("parse_status", "TEXT", 1, "'pending'", 0),
    ("parse_error", "TEXT", 0, None, 0),
)


@dataclass(frozen=True)
class _RawEventsSchemaState:
    raw_events_exists: bool
    raw_events_needs_migration: bool
    raw_events_new_exists: bool
    raw_events_new_matches_target: bool
    dependent_views: tuple[str, ...]


# ── Connection helpers ──────────────────────────────────────────────────────
async def create_db_connection(
    db_path: Path | None = None,
) -> aiosqlite.Connection:
    db = await aiosqlite.connect(str(db_path or PARSED_DB))
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.execute("PRAGMA busy_timeout=5000;")
    await db.execute("PRAGMA foreign_keys=ON;")
    return db


@asynccontextmanager
async def open_db(db_path: Path | None = None) -> aiosqlite.Connection:
    db = await create_db_connection(db_path)
    try:
        yield db
    finally:
        await db.close()


def retry_on_locked(max_retries: int = 5, base_delay: float = 0.1):
    """Retry with exponential backoff when DB is locked."""
    def decorator(func):
        signature = inspect.signature(func)

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                bound = signature.bind_partial(*args, **kwargs)
            except TypeError:
                bound = None
            if bound is not None and bound.arguments.get("db") is not None:
                return await func(*args, **kwargs)

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


def _normalize_schema_sql(sql: Optional[str]) -> str:
    if not sql:
        return ""
    return " ".join(str(sql).split()).upper()


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _fetch_sqlite_master_sql(
    db: aiosqlite.Connection,
    object_type: str,
    name: str,
) -> Optional[str]:
    async with db.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = ? AND name = ?
        """,
        (object_type, name),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        return None
    return row[0]


def _raw_events_sql_needs_migration(sql: Optional[str]) -> bool:
    return "UNIQUE (SOURCE, SOURCE_EVENT_ID)" in _normalize_schema_sql(sql)


async def _get_table_info(
    db: aiosqlite.Connection,
    table_name: str,
) -> tuple[tuple[str, str, int, Optional[str], int], ...]:
    pragma = f"PRAGMA table_info({_quote_identifier(table_name)})"
    async with db.execute(pragma) as cursor:
        rows = await cursor.fetchall()
    return tuple(
        (
            str(row[1]),
            str(row[2]).upper(),
            int(row[3]),
            row[4],
            int(row[5]),
        )
        for row in rows
    )


async def _table_has_unique_index(
    db: aiosqlite.Connection,
    table_name: str,
    columns: tuple[str, ...],
    *,
    partial: Optional[bool] = None,
) -> bool:
    pragma = f"PRAGMA index_list({_quote_identifier(table_name)})"
    async with db.execute(pragma) as cursor:
        indexes = await cursor.fetchall()

    for index_row in indexes:
        index_name = str(index_row[1])
        is_unique = bool(index_row[2])
        is_partial = bool(index_row[4]) if len(index_row) > 4 else False
        if not is_unique:
            continue
        if partial is not None and is_partial != partial:
            continue

        index_info_pragma = f"PRAGMA index_info({_quote_identifier(index_name)})"
        async with db.execute(index_info_pragma) as index_cursor:
            index_columns = tuple(str(row[2]) for row in await index_cursor.fetchall())
        if index_columns == columns:
            return True

    return False


async def _table_matches_target_raw_events_schema(
    db: aiosqlite.Connection,
    table_name: str,
) -> bool:
    if await _get_table_info(db, table_name) != _RAW_EVENTS_EXPECTED_TABLE_INFO:
        return False
    if not await _table_has_unique_index(
        db,
        table_name,
        ("raw_hash",),
        partial=False,
    ):
        return False
    return not await _table_has_unique_index(
        db,
        table_name,
        ("source", "source_event_id"),
        partial=False,
    )


async def _list_raw_events_dependent_views(
    db: aiosqlite.Connection,
) -> tuple[str, ...]:
    async with db.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'view' AND sql IS NOT NULL
        """
    ) as cursor:
        rows = await cursor.fetchall()

    return tuple(
        sorted(
            str(name)
            for name, sql in rows
            if "RAW_EVENTS" in _normalize_schema_sql(sql)
        )
    )


async def _inspect_raw_events_schema_state(
    db: aiosqlite.Connection,
) -> _RawEventsSchemaState:
    raw_events_sql = await _fetch_sqlite_master_sql(db, "table", "raw_events")
    raw_events_new_sql = await _fetch_sqlite_master_sql(db, "table", "raw_events_new")
    return _RawEventsSchemaState(
        raw_events_exists=raw_events_sql is not None,
        raw_events_needs_migration=_raw_events_sql_needs_migration(raw_events_sql),
        raw_events_new_exists=raw_events_new_sql is not None,
        raw_events_new_matches_target=(
            await _table_matches_target_raw_events_schema(db, "raw_events_new")
            if raw_events_new_sql is not None
            else False
        ),
        dependent_views=await _list_raw_events_dependent_views(db),
    )


async def _drop_views(
    db: aiosqlite.Connection,
    view_names: tuple[str, ...],
) -> None:
    for view_name in view_names:
        await db.execute(f"DROP VIEW IF EXISTS {_quote_identifier(view_name)};")


async def _promote_raw_events_new(db: aiosqlite.Connection) -> None:
    state = await _inspect_raw_events_schema_state(db)
    if not state.raw_events_new_exists:
        return

    await db.commit()
    await db.execute("PRAGMA foreign_keys=OFF;")
    try:
        await db.execute("BEGIN IMMEDIATE;")
        await _drop_views(db, state.dependent_views)
        await db.execute("ALTER TABLE raw_events_new RENAME TO raw_events;")
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.commit()


async def _repair_raw_events_schema_state(
    db: aiosqlite.Connection,
) -> _RawEventsSchemaState:
    state = await _inspect_raw_events_schema_state(db)

    if state.raw_events_exists and state.raw_events_new_exists:
        logger.warning(
            "Found stale raw_events_new alongside raw_events; discarding temp table"
        )
        await db.execute("DROP TABLE IF EXISTS raw_events_new;")
        await db.commit()
        return await _inspect_raw_events_schema_state(db)

    if not state.raw_events_exists and state.raw_events_new_exists:
        if not state.raw_events_new_matches_target:
            raise RuntimeError(
                "Detected interrupted raw_events migration, but raw_events_new "
                "does not match the expected schema. Manual repair required."
            )
        logger.warning(
            "Detected interrupted raw_events migration; promoting raw_events_new"
        )
        await _promote_raw_events_new(db)
        return await _inspect_raw_events_schema_state(db)

    return state


async def _migrate_raw_events_schema(db: aiosqlite.Connection) -> None:
    state = await _inspect_raw_events_schema_state(db)
    if not state.raw_events_exists or not state.raw_events_needs_migration:
        return

    logger.info("Migrating raw_events to source-aware dedupe rules")

    await db.commit()
    await db.execute("PRAGMA foreign_keys=OFF;")
    try:
        await db.execute("BEGIN IMMEDIATE;")
        await _drop_views(db, state.dependent_views)
        await db.execute("DROP TABLE IF EXISTS raw_events_new;")
        await db.execute(
            """
            CREATE TABLE raw_events_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                source_event_id TEXT,
                received_at TEXT NOT NULL,
                raw_payload TEXT NOT NULL,
                raw_hash TEXT NOT NULL UNIQUE,
                transport_meta_json TEXT,
                parse_status TEXT NOT NULL DEFAULT 'pending',
                parse_error TEXT
            );
            """
        )
        await db.execute(
            """
            INSERT INTO raw_events_new
                (id, source, source_event_id, received_at, raw_payload, raw_hash,
                 transport_meta_json, parse_status, parse_error)
            SELECT
                id, source, source_event_id, received_at, raw_payload, raw_hash,
                transport_meta_json, parse_status, parse_error
            FROM raw_events
            """
        )
        await db.execute("DROP TABLE raw_events;")
        await db.execute("ALTER TABLE raw_events_new RENAME TO raw_events;")
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.execute("PRAGMA foreign_keys=ON;")
        await db.commit()


async def _raw_events_needs_migration(db: aiosqlite.Connection) -> bool:
    state = await _inspect_raw_events_schema_state(db)
    return state.raw_events_needs_migration


async def init_db(db_path: Path | None = None) -> None:
    """Create tables, indexes, and views if they don't exist."""
    # NOTE: Legacy "mensajes" table is no longer written. It remains for backwards compatibility.
    async with open_db(db_path) as db:
        state = await _repair_raw_events_schema_state(db)
        if state.raw_events_needs_migration:
            await _migrate_raw_events_schema(db)
        await db.execute(_CREATE_RAW_EVENTS)
        await db.execute(_CREATE_NORMALIZED_EVENTS)
        await db.execute(_CREATE_PRUEBAS)
        await db.execute(_CREATE_ALERTAS)
        await db.execute(_CREATE_RESOLVED_SLOTS)
        for idx_sql in _CREATE_INDEXES:
            await db.execute(idx_sql)
        for view_sql in _CREATE_VIEWS:
            await db.execute(view_sql)
        for drop_sql in _DROP_TRIGGERS:
            await db.execute(drop_sql)
        # Legacy successful rows used "parsed"; normalize them to the finalized status.
        await db.execute(
            """
            UPDATE raw_events
            SET parse_status = 'complete'
            WHERE parse_status = 'parsed'
            """
        )
        await db.commit()
    logger.info("Database initialized: %s", db_path or PARSED_DB)


def _hash_raw_event(raw: RawEvent) -> str:
    src = raw.source.value
    src_id = raw.source_event_id or ""
    payload = raw.raw_payload or ""
    value = f"{src}|{src_id}|{payload}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()


async def _fetch_raw_event_row_by_hash(
    db: aiosqlite.Connection,
    raw_hash: str,
) -> tuple[int, str] | None:
    async with db.execute(
        """
        SELECT id, parse_status
        FROM raw_events
        WHERE raw_hash = ?
        """,
        (raw_hash,),
    ) as cursor:
        row = await cursor.fetchone()
    if not row:
        return None
    return int(row[0]), str(row[1])


@retry_on_locked()
async def insert_raw_event(
    raw: RawEvent,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> tuple[bool, Optional[int]]:
    """Insert raw event. Returns (inserted, raw_event_id)."""
    raw_hash = _hash_raw_event(raw)
    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        cursor = await db.execute(
            """
            INSERT OR IGNORE INTO raw_events
                (source, source_event_id, received_at, raw_payload, raw_hash, transport_meta_json)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (
                raw.source.value,
                raw.source_event_id,
                raw.received_at.isoformat(),
                raw.raw_payload,
                raw_hash,
                json.dumps(raw.transport_meta or {}, ensure_ascii=True),
            ),
        )
        if owns_db:
            await db.commit()
        if cursor.rowcount == 0:
            return False, None
        return True, cursor.lastrowid
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def update_raw_event_status(
    raw_event_id: int,
    status: str,
    error: Optional[str],
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> None:
    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        await db.execute(
            "UPDATE raw_events SET parse_status = ?, parse_error = ? WHERE id = ?",
            (status, error, raw_event_id),
        )
        if owns_db:
            await db.commit()
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def record_processing_failure(
    raw: RawEvent,
    *,
    raw_event_id: int | None = None,
    exception: Exception | None = None,
    db_path: Path | None = None,
) -> int | None:
    """
    Preserve the raw payload when transactional processing fails unexpectedly.

    This always runs in its own transaction so it can persist audit evidence after
    the hot-path transaction has already rolled back.
    """
    parse_error = "processing_failure"
    if exception is not None:
        parse_error = f"processing_failure:{type(exception).__name__}"

    async with open_db(db_path) as db:
        target_raw_event_id = raw_event_id
        raw_hash = _hash_raw_event(raw)

        if target_raw_event_id is None:
            inserted, target_raw_event_id = await insert_raw_event(raw, db=db)
            if not inserted:
                existing = await _fetch_raw_event_row_by_hash(db, raw_hash)
                if existing is None:
                    raise RuntimeError(
                        "Failed to recover raw_event_id for processing failure fallback"
                    )
                target_raw_event_id, existing_status = existing
                if existing_status in _FINALIZED_RAW_STATUSES:
                    return target_raw_event_id

        await update_raw_event_status(
            target_raw_event_id,
            "error",
            parse_error,
            db=db,
        )
        await db.commit()
        return target_raw_event_id


@retry_on_locked()
async def get_latest_telegram_message_id(
    db_path: Path | None = None,
) -> Optional[int]:
    """Return the highest persisted numeric Telegram message id, if any."""
    async with open_db(db_path) as db:
        async with db.execute(
            """
            SELECT MAX(CAST(source_event_id AS INTEGER))
            FROM raw_events
            WHERE source = 'telegram'
              AND parse_status IN ('complete', 'error', 'partial')
              AND source_event_id IS NOT NULL
              AND source_event_id != ''
              AND source_event_id NOT GLOB '*[^0-9]*'
            """
        ) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] is None:
                return None
            return int(row[0])


def _load_payload_json(payload_json: Optional[str]) -> dict:
    if not payload_json:
        return {}
    try:
        payload = json.loads(payload_json)
    except (TypeError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _extract_state_pair_event_id(payload: dict) -> Optional[int]:
    value = payload.get(_STATE_PAIR_EVENT_ID_KEY)
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _clear_state_pair_metadata(payload: dict) -> dict:
    cleaned = dict(payload)
    for key in (
        _STATE_PAIR_EVENT_ID_KEY,
        _STATE_PAIR_EVENT_TYPE_KEY,
        _STATE_PAIR_DELTA_SECONDS_KEY,
        _STATE_PAIR_DIRECTION_KEY,
        _STATE_PAIR_WINDOW_SECONDS_KEY,
    ):
        cleaned.pop(key, None)
    return cleaned


def _clear_state_pair_promotion_metadata(payload: dict) -> dict:
    cleaned = dict(payload)
    for key in (
        _STATE_PAIR_PROMOTED_KEY,
        _STATE_PAIR_ORIGINAL_REPORT_SLOT_KEY,
        _STATE_PAIR_ORIGINAL_TONE_KEY,
        _STATE_PAIR_ORIGINAL_IS_VALID_REPORT_KEY,
    ):
        cleaned.pop(key, None)
    return cleaned


def _set_state_pair_metadata(
    payload: dict,
    paired_event_id: int,
    paired_event_type: str,
    delta_seconds: int,
    direction: str,
) -> dict:
    updated = _clear_state_pair_metadata(payload)
    updated[_STATE_PAIR_EVENT_ID_KEY] = paired_event_id
    updated[_STATE_PAIR_EVENT_TYPE_KEY] = paired_event_type
    updated[_STATE_PAIR_DELTA_SECONDS_KEY] = delta_seconds
    updated[_STATE_PAIR_DIRECTION_KEY] = direction
    updated[_STATE_PAIR_WINDOW_SECONDS_KEY] = STATE_PAIR_WINDOW_SECONDS
    return updated


def _pair_direction(source_time: datetime, other_time: datetime) -> str:
    if other_time > source_time:
        return "after"
    if other_time < source_time:
        return "before"
    return "same_time"


def _is_state_pair_promoted(payload: dict) -> bool:
    return bool(payload.get(_STATE_PAIR_PROMOTED_KEY))


def _set_state_pair_promotion_metadata(
    payload: dict,
    *,
    original_report_slot: Optional[str],
    original_tone: int,
    original_is_valid_report: int,
) -> dict:
    updated = dict(payload)
    updated[_STATE_PAIR_PROMOTED_KEY] = True
    updated[_STATE_PAIR_ORIGINAL_REPORT_SLOT_KEY] = original_report_slot
    updated[_STATE_PAIR_ORIGINAL_TONE_KEY] = int(original_tone)
    updated[_STATE_PAIR_ORIGINAL_IS_VALID_REPORT_KEY] = int(original_is_valid_report)
    return updated


async def _fetch_normalized_event_for_pairing(
    db: aiosqlite.Connection,
    event_id: int,
) -> Optional[tuple]:
    async with db.execute(
        """
        SELECT id, source, station_id, report_date, report_slot, event_type,
               event_class, tone, is_valid_report, event_time_local, payload_json
        FROM normalized_events
        WHERE id = ?
        """,
        (event_id,),
    ) as cursor:
        return await cursor.fetchone()


async def _update_normalized_payload_json(
    db: aiosqlite.Connection,
    event_id: int,
    payload: dict,
) -> None:
    await db.execute(
        "UPDATE normalized_events SET payload_json = ? WHERE id = ?",
        (json.dumps(payload, ensure_ascii=True), event_id),
    )


async def _sync_projection_tables_for_event(
    db: aiosqlite.Connection,
    event_id: int,
) -> None:
    async with db.execute(
        """
        SELECT source, station_id, station_name, station_code, report_date, report_slot,
               event_type, event_class, tone, event_time_local, event_time_utc,
               confidence_score, parser_version, transmitter_code, phone_number,
               channel, payload_json
        FROM normalized_events
        WHERE id = ?
        """,
        (event_id,),
    ) as cursor:
        row = await cursor.fetchone()

    await db.execute(
        "DELETE FROM pruebas WHERE normalized_event_id = ?",
        (event_id,),
    )
    await db.execute(
        "DELETE FROM alertas WHERE normalized_event_id = ?",
        (event_id,),
    )

    if not row:
        return

    (
        source,
        station_id,
        station_name,
        station_code,
        report_date,
        report_slot,
        event_type,
        event_class,
        tone,
        event_time_local,
        event_time_utc,
        confidence_score,
        parser_version,
        transmitter_code,
        phone_number,
        channel,
        payload_json,
    ) = row

    target_table: Optional[str] = None
    if report_slot is None and event_type == "EQW":
        target_table = "alertas"
    elif report_slot is None and int(tone or 0) == 0:
        target_table = "pruebas"

    if not target_table:
        return

    await db.execute(
        f"""
        INSERT INTO {target_table}
            (normalized_event_id, source, station_id, station_name, station_code,
             report_date, report_slot, event_type, event_class, tone,
             event_time_local, event_time_utc, confidence_score, parser_version,
             transmitter_code, phone_number, channel, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        """,
        (
            event_id,
            source,
            station_id,
            station_name,
            station_code,
            report_date,
            report_slot,
            event_type,
            event_class,
            tone,
            event_time_local,
            event_time_utc,
            confidence_score,
            parser_version,
            transmitter_code,
            phone_number,
            channel,
            payload_json,
        ),
    )


async def _update_normalized_state_fields(
    db: aiosqlite.Connection,
    event_id: int,
    *,
    report_slot: Optional[str],
    tone: int,
    is_valid_report: int,
) -> None:
    await db.execute(
        """
        UPDATE normalized_events
        SET report_slot = ?, tone = ?, is_valid_report = ?
        WHERE id = ?
        """,
        (report_slot, tone, is_valid_report, event_id),
    )
    await _sync_projection_tables_for_event(db, event_id)


async def _revert_promoted_state_fields_if_needed(
    db: aiosqlite.Connection,
    event_id: int,
    payload: dict,
) -> None:
    if not _is_state_pair_promoted(payload):
        return

    original_tone = int(payload.get(_STATE_PAIR_ORIGINAL_TONE_KEY, 0))
    original_is_valid_report = int(payload.get(_STATE_PAIR_ORIGINAL_IS_VALID_REPORT_KEY, 0))
    await _update_normalized_state_fields(
        db,
        event_id,
        report_slot=payload.get(_STATE_PAIR_ORIGINAL_REPORT_SLOT_KEY),
        tone=original_tone,
        is_valid_report=original_is_valid_report,
    )


async def _clear_state_pair_metadata_for_event(
    db: aiosqlite.Connection,
    event_id: int,
) -> None:
    row = await _fetch_normalized_event_for_pairing(db, event_id)
    if not row:
        return
    payload = _load_payload_json(row[10])
    await _revert_promoted_state_fields_if_needed(db, event_id, payload)
    cleaned = _clear_state_pair_promotion_metadata(_clear_state_pair_metadata(payload))
    if cleaned != payload:
        await _update_normalized_payload_json(db, event_id, cleaned)


async def _unlink_state_pair(
    db: aiosqlite.Connection,
    event_id: int,
) -> None:
    row = await _fetch_normalized_event_for_pairing(db, event_id)
    if not row:
        return

    payload = _load_payload_json(row[10])
    paired_event_id = _extract_state_pair_event_id(payload)
    cleaned = _clear_state_pair_promotion_metadata(_clear_state_pair_metadata(payload))
    if cleaned != payload:
        await _update_normalized_payload_json(db, event_id, cleaned)

    if paired_event_id is None:
        return

    paired_row = await _fetch_normalized_event_for_pairing(db, paired_event_id)
    if not paired_row:
        return

    paired_payload = _load_payload_json(paired_row[10])
    if _extract_state_pair_event_id(paired_payload) != event_id:
        return

    await _revert_promoted_state_fields_if_needed(db, paired_event_id, paired_payload)
    paired_cleaned = _clear_state_pair_promotion_metadata(
        _clear_state_pair_metadata(paired_payload)
    )
    if paired_cleaned != paired_payload:
        await _update_normalized_payload_json(db, paired_event_id, paired_cleaned)


async def _pair_state_event(
    db: aiosqlite.Connection,
    event_id: int,
) -> None:
    row = await _fetch_normalized_event_for_pairing(db, event_id)
    if not row:
        return

    (
        _event_id,
        source,
        station_id,
        report_date,
        report_slot,
        event_type,
        event_class,
        tone,
        is_valid_report,
        event_time_local,
        payload_json,
    ) = row
    if (
        source != "telegram"
        or event_class != "STATE"
        or station_id is None
        or report_date is None
        or event_type not in _PAIRABLE_STATE_TYPES
    ):
        return

    payload = _load_payload_json(payload_json)
    if _extract_state_pair_event_id(payload) is not None:
        return

    event_time = datetime.fromisoformat(event_time_local)
    opposite_type = "CLOSE" if event_type == "OPEN" else "OPEN"

    async with db.execute(
        """
        SELECT id, report_slot, event_type, tone, is_valid_report, event_time_local, payload_json
        FROM normalized_events
        WHERE id != ?
          AND source = 'telegram'
          AND event_class = 'STATE'
          AND station_id = ?
          AND report_date = ?
          AND event_type = ?
        """,
        (event_id, station_id, report_date, opposite_type),
    ) as cursor:
        candidates = await cursor.fetchall()

    ranked_candidates: list[tuple[int, datetime, int, Optional[str], str, int, int, dict]] = []
    for (
        candidate_id,
        candidate_report_slot,
        candidate_type,
        candidate_tone,
        candidate_is_valid_report,
        candidate_time_local,
        candidate_payload_json,
    ) in candidates:
        candidate_payload = _load_payload_json(candidate_payload_json)
        if _extract_state_pair_event_id(candidate_payload) is not None:
            continue

        candidate_time = datetime.fromisoformat(candidate_time_local)
        delta_seconds = int(abs((candidate_time - event_time).total_seconds()))
        if delta_seconds > STATE_PAIR_WINDOW_SECONDS:
            continue

        ranked_candidates.append(
            (
                delta_seconds,
                candidate_time,
                candidate_id,
                candidate_report_slot,
                candidate_type,
                int(candidate_tone),
                int(candidate_is_valid_report),
                candidate_payload,
            )
        )

    if not ranked_candidates:
        await _clear_state_pair_metadata_for_event(db, event_id)
        return

    ranked_candidates.sort(key=lambda item: (item[0], item[1], item[2]))
    (
        delta_seconds,
        candidate_time,
        candidate_id,
        candidate_report_slot,
        candidate_type,
        candidate_tone,
        candidate_is_valid_report,
        candidate_payload,
    ) = ranked_candidates[0]

    promoted_payload = payload
    promoted_candidate_payload = candidate_payload
    if (
        report_slot is None
        and candidate_report_slot is not None
        and candidate_tone
        and candidate_is_valid_report
        and event_time > candidate_time
    ):
        promoted_payload = _set_state_pair_promotion_metadata(
            promoted_payload,
            original_report_slot=report_slot,
            original_tone=int(tone),
            original_is_valid_report=int(is_valid_report),
        )
        await _update_normalized_state_fields(
            db,
            event_id,
            report_slot=candidate_report_slot,
            tone=1,
            is_valid_report=1,
        )
    elif (
        candidate_report_slot is None
        and report_slot is not None
        and tone
        and is_valid_report
        and candidate_time > event_time
    ):
        promoted_candidate_payload = _set_state_pair_promotion_metadata(
            promoted_candidate_payload,
            original_report_slot=candidate_report_slot,
            original_tone=int(candidate_tone),
            original_is_valid_report=int(candidate_is_valid_report),
        )
        await _update_normalized_state_fields(
            db,
            candidate_id,
            report_slot=report_slot,
            tone=1,
            is_valid_report=1,
        )

    updated_payload = _set_state_pair_metadata(
        promoted_payload,
        paired_event_id=candidate_id,
        paired_event_type=candidate_type,
        delta_seconds=delta_seconds,
        direction=_pair_direction(event_time, candidate_time),
    )
    updated_candidate_payload = _set_state_pair_metadata(
        promoted_candidate_payload,
        paired_event_id=event_id,
        paired_event_type=event_type,
        delta_seconds=delta_seconds,
        direction=_pair_direction(candidate_time, event_time),
    )

    await _update_normalized_payload_json(db, event_id, updated_payload)
    await _update_normalized_payload_json(db, candidate_id, updated_candidate_payload)


@retry_on_locked()
async def insert_normalized_event(
    event: NormalizedEvent,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> int:
    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        cursor = await db.execute(
            """
            INSERT INTO normalized_events
                (raw_event_id, source, station_id, station_name, station_code,
                 report_date, report_slot, event_type, event_class, channel,
                 tone, priority, is_valid_report, event_time_utc, event_time_local,
                 confidence_score, parser_version, transmitter_code, phone_number, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                event.raw_event_id,
                event.source.value,
                event.station_id,
                event.station_name,
                event.station_code,
                event.report_date,
                event.report_slot,
                event.event_type,
                event.event_class,
                event.channel,
                int(event.tone),
                event.priority,
                int(event.is_valid_report),
                event.event_time_utc,
                event.event_time_local,
                event.confidence_score,
                event.parser_version,
                event.transmitter_code,
                event.phone_number,
                json.dumps(event.payload_json or {}, ensure_ascii=True),
            ),
        )
        normalized_event_id = cursor.lastrowid
        await _pair_state_event(db, normalized_event_id)
        await _sync_projection_tables_for_event(db, normalized_event_id)
        if owns_db:
            await db.commit()
        return normalized_event_id
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def delete_normalized_events_for_raw(
    raw_event_id: int,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> None:
    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        async with db.execute(
            "SELECT id FROM normalized_events WHERE raw_event_id = ?",
            (raw_event_id,),
        ) as cursor:
            event_ids = [row[0] for row in await cursor.fetchall()]

        for event_id in event_ids:
            await _unlink_state_pair(db, event_id)
            await db.execute(
                "DELETE FROM pruebas WHERE normalized_event_id = ?",
                (event_id,),
            )
            await db.execute(
                "DELETE FROM alertas WHERE normalized_event_id = ?",
                (event_id,),
            )

        await db.execute(
            "DELETE FROM normalized_events WHERE raw_event_id = ?",
            (raw_event_id,),
        )
        if owns_db:
            await db.commit()
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def get_slots_for_raw(
    raw_event_id: int,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> set[tuple[int, str, str]]:
    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        async with db.execute(
            """
            SELECT station_id, report_date, report_slot
            FROM normalized_events
            WHERE raw_event_id = ?
            """,
            (raw_event_id,),
        ) as cursor:
            rows = await cursor.fetchall()
        return {
            (int(station_id), str(report_date), str(report_slot))
            for station_id, report_date, report_slot in rows
            if station_id and report_date and report_slot
        }
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def rebuild_projection_tables(
    db_path: Path | None = None,
) -> None:
    """Rebuild pruebas and alertas from current normalized_events state."""
    async with open_db(db_path) as db:
        await db.execute("DELETE FROM pruebas")
        await db.execute("DELETE FROM alertas")

        await db.execute(
            """
            INSERT INTO pruebas
                (normalized_event_id, source, station_id, station_name, station_code,
                 report_date, report_slot, event_type, event_class, tone,
                 event_time_local, event_time_utc, confidence_score, parser_version,
                 transmitter_code, phone_number, channel, payload_json, created_at)
            SELECT id, source, station_id, station_name, station_code,
                   report_date, report_slot, event_type, event_class, tone,
                   event_time_local, event_time_utc, confidence_score, parser_version,
                   transmitter_code, phone_number, channel, payload_json, datetime('now')
            FROM normalized_events
            WHERE report_slot IS NULL
              AND tone = 0
              AND event_type != 'EQW'
            """
        )

        await db.execute(
            """
            INSERT INTO alertas
                (normalized_event_id, source, station_id, station_name, station_code,
                 report_date, report_slot, event_type, event_class, tone,
                 event_time_local, event_time_utc, confidence_score, parser_version,
                 transmitter_code, phone_number, channel, payload_json, created_at)
            SELECT id, source, station_id, station_name, station_code,
                   report_date, report_slot, event_type, event_class, tone,
                   event_time_local, event_time_utc, confidence_score, parser_version,
                   transmitter_code, phone_number, channel, payload_json, datetime('now')
            FROM normalized_events
            WHERE report_slot IS NULL
              AND event_type = 'EQW'
            """
        )

        await db.commit()


@retry_on_locked()
async def enforce_first_valid_for_slot(
    station_id: int,
    report_date: str,
    report_slot: str,
    event_type: str,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> None:
    """
    Enforce "first arrival wins" for a given slot.
    Only the earliest received_at event of the given event_type keeps is_valid_report=1.
    """
    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        await db.execute(
            """
            UPDATE normalized_events
            SET is_valid_report = CASE
                WHEN id = (
                    SELECT ne.id
                    FROM normalized_events ne
                    JOIN raw_events re ON re.id = ne.raw_event_id
                    WHERE ne.station_id = ?
                      AND ne.report_date = ?
                      AND ne.report_slot = ?
                      AND ne.event_type = ?
                      AND ne.tone = 1
                    ORDER BY re.received_at ASC, ne.id ASC
                    LIMIT 1
                ) THEN 1 ELSE 0 END
            WHERE station_id = ?
              AND report_date = ?
              AND report_slot = ?
              AND event_type = ?
              AND tone = 1
            """,
            (
                station_id,
                report_date,
                report_slot,
                event_type,
                station_id,
                report_date,
                report_slot,
                event_type,
            ),
        )
        if owns_db:
            await db.commit()
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def has_open_and_close(
    station_id: int,
    start: datetime,
    end: datetime,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> bool:
    """
    Check if valid OPEN and CLOSE exist for a station in the target report window.

    Rows promoted into a report slot via state pairing may fall outside the raw
    timestamp window, so we count both the physical time window and the stored
    report slot that corresponds to the window start.
    """
    report_date = start.date().isoformat()
    report_slot = start.strftime("%H:%M")

    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        async with db.execute(
            """
            SELECT COUNT(DISTINCT event_type) FROM normalized_events
            WHERE station_id = ?
              AND event_type IN ('OPEN', 'CLOSE')
              AND is_valid_report = 1
              AND (
                    event_time_local BETWEEN ? AND ?
                 OR (report_date = ? AND report_slot = ?)
              )
            """,
            (
                station_id,
                start.strftime("%Y-%m-%d %H:%M:%S"),
                end.strftime("%Y-%m-%d %H:%M:%S"),
                report_date,
                report_slot,
            ),
        ) as cursor:
            row = await cursor.fetchone()
            return row is not None and row[0] == 2
    finally:
        if owns_db:
            await db.close()


@retry_on_locked()
async def get_events_by_station_date(
    station_name: str,
    date_str: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Fetch normalized events for a station by report_date (tono window only)."""
    async with open_db(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT * FROM normalized_events
            WHERE station_name = ? AND report_date = ?
              AND report_slot IS NOT NULL
            ORDER BY event_time_local
            """,
            (station_name, date_str),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


@retry_on_locked()
async def get_events_by_station_local_date(
    station_name: str,
    date_str: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Fetch normalized events by local calendar date derived from event_time_local (YYYY-MM-DD)."""
    async with open_db(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT * FROM normalized_events
            WHERE station_name = ? AND date(event_time_local) = ?
            ORDER BY event_time_local
            """,
            (station_name, date_str),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_messages_by_station_date(
    station_name: str,
    date_str: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Compatibility wrapper for legacy callers (report_date-based)."""
    return await get_events_by_station_date(station_name, date_str, db_path=db_path)


async def get_messages_by_station_local_date(
    station_name: str,
    date_str: str,
    db_path: Path | None = None,
) -> list[dict]:
    """Compatibility wrapper for local-date queries."""
    return await get_events_by_station_local_date(
        station_name,
        date_str,
        db_path=db_path,
    )
