"""
resolution_engine.py - Deterministic projection for resolved report slots.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

from persistence import create_db_connection, retry_on_locked


@retry_on_locked()
async def resolve_slot(
    station_id: int,
    report_date: str,
    report_slot: str,
    db_path: Path | None = None,
    db: aiosqlite.Connection | None = None,
) -> None:
    if station_id is None or report_date is None or report_slot is None:
        return

    owns_db = db is None
    if db is None:
        db = await create_db_connection(db_path)
    try:
        async with db.execute(
            """
            SELECT id, source, event_type, confidence_score, station_name,
                   event_time_local, priority
            FROM normalized_events
            WHERE station_id = ? AND report_date = ? AND report_slot = ? AND is_valid_report = 1
            ORDER BY priority DESC, confidence_score DESC, event_time_local ASC, id ASC
            """,
            (station_id, report_date, report_slot),
        ) as cursor:
            winners = await cursor.fetchall()

        if not winners:
            await db.execute(
                """
                DELETE FROM resolved_report_slots
                WHERE station_id = ? AND report_date = ? AND report_slot = ?
                """,
                (station_id, report_date, report_slot),
            )
            await db.commit()
            return

        valid_opens = [
            row for row in winners if row[2] == "OPEN"
        ]
        valid_closes = [
            row for row in winners if row[2] == "CLOSE"
        ]
        if valid_opens and valid_closes:
            winner = min(valid_opens, key=lambda row: (row[5], row[0]))
        else:
            winner = winners[0]

        winner_id, source, event_type, confidence, station_name, event_time_local, _priority = winner

        async with db.execute(
            """
            SELECT first_seen_at FROM resolved_report_slots
            WHERE station_id = ? AND report_date = ? AND report_slot = ?
            """,
            (station_id, report_date, report_slot),
        ) as cursor:
            existing = await cursor.fetchone()
            first_seen_at = existing[0] if existing else event_time_local

        now = datetime.now(timezone.utc).isoformat()

        await db.execute(
            """
            INSERT INTO resolved_report_slots
                (station_id, station_name, report_date, report_slot,
                 effective_event_id, effective_source, effective_event_type,
                 effective_confidence, first_seen_at, last_updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id, report_date, report_slot)
            DO UPDATE SET
                station_name = excluded.station_name,
                effective_event_id = excluded.effective_event_id,
                effective_source = excluded.effective_source,
                effective_event_type = excluded.effective_event_type,
                effective_confidence = excluded.effective_confidence,
                first_seen_at = excluded.first_seen_at,
                last_updated_at = excluded.last_updated_at
            """,
            (
                station_id,
                station_name,
                report_date,
                report_slot,
                winner_id,
                source,
                event_type,
                confidence,
                first_seen_at,
                now,
            ),
        )
        if owns_db:
            await db.commit()
    finally:
        if owns_db:
            await db.close()
