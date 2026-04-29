import json
import sqlite3
from datetime import datetime, timedelta

import pytest

from models import NormalizedEvent, RawEvent, Source
from persistence import (
    delete_normalized_events_for_raw,
    enforce_first_valid_for_slot,
    get_events_by_station_date,
    get_events_by_station_local_date,
    get_latest_telegram_message_id,
    get_slots_for_raw,
    has_open_and_close,
    init_db,
    insert_normalized_event,
    insert_raw_event,
    open_db,
    update_raw_event_status,
)


def _create_raw_events_table(
    conn: sqlite3.Connection,
    *,
    table_name: str = "raw_events",
    legacy_unique: bool = False,
) -> None:
    unique_clause = ",\n                UNIQUE (source, source_event_id)" if legacy_unique else ""
    conn.execute(
        f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_event_id TEXT,
            received_at TEXT NOT NULL,
            raw_payload TEXT NOT NULL,
            raw_hash TEXT NOT NULL UNIQUE,
            transport_meta_json TEXT,
            parse_status TEXT NOT NULL DEFAULT 'pending',
            parse_error TEXT
            {unique_clause}
        )
        """
    )


def _insert_raw_event_row(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    source: str,
    source_event_id: str,
    raw_payload: str,
    raw_hash: str,
) -> None:
    conn.execute(
        f"""
        INSERT INTO {table_name}
            (source, source_event_id, received_at, raw_payload, raw_hash,
             transport_meta_json, parse_status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source,
            source_event_id,
            "2026-03-23T12:00:00+00:00",
            raw_payload,
            raw_hash,
            "{}",
            "pending",
        ),
    )


async def _assert_source_specific_dedupe(parsed_db_path):
    serial_a = RawEvent(
        source=Source.SERIAL,
        source_event_id="COM3-1",
        raw_payload="SERIAL_FRAME_A",
        received_at=datetime(2026, 3, 23, 12, 0, 0),
        transport_meta={},
    )
    serial_b = RawEvent(
        source=Source.SERIAL,
        source_event_id="COM3-1",
        raw_payload="SERIAL_FRAME_B",
        received_at=datetime(2026, 3, 23, 12, 1, 0),
        transport_meta={},
    )
    telegram_a = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="tg-1",
        raw_payload="FIRST",
        received_at=datetime(2026, 3, 23, 12, 2, 0),
        transport_meta={},
    )
    telegram_b = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="tg-1",
        raw_payload="SECOND",
        received_at=datetime(2026, 3, 23, 12, 3, 0),
        transport_meta={},
    )

    assert (await insert_raw_event(serial_a, db_path=parsed_db_path))[0] is True
    assert (await insert_raw_event(serial_b, db_path=parsed_db_path))[0] is True
    assert (await insert_raw_event(telegram_a, db_path=parsed_db_path))[0] is True
    assert (await insert_raw_event(telegram_b, db_path=parsed_db_path))[0] is False

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM raw_events WHERE source = ?",
            (Source.SERIAL.value,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 2

        async with db.execute(
            "SELECT COUNT(*) FROM raw_events WHERE source = ?",
            (Source.TELEGRAM.value,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1


@pytest.mark.asyncio
async def test_get_latest_telegram_message_id_ignores_non_numeric_rows(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    rows = [
        RawEvent(
            source=Source.TELEGRAM,
            source_event_id="42",
            raw_payload="NUMERIC_EARLY",
            received_at=datetime(2026, 3, 23, 12, 0, 0),
            transport_meta={},
        ),
        RawEvent(
            source=Source.TELEGRAM,
            source_event_id="tg-1",
            raw_payload="NON_NUMERIC",
            received_at=datetime(2026, 3, 23, 12, 1, 0),
            transport_meta={},
        ),
        RawEvent(
            source=Source.TELEGRAM,
            source_event_id=None,
            raw_payload="NULL_ID",
            received_at=datetime(2026, 3, 23, 12, 2, 0),
            transport_meta={},
        ),
        RawEvent(
            source=Source.SERIAL,
            source_event_id="999",
            raw_payload="SERIAL_NUMERIC",
            received_at=datetime(2026, 3, 23, 12, 3, 0),
            transport_meta={},
        ),
        RawEvent(
            source=Source.TELEGRAM,
            source_event_id="105",
            raw_payload="NUMERIC_LATE",
            received_at=datetime(2026, 3, 23, 12, 4, 0),
            transport_meta={},
        ),
    ]

    for raw in rows:
        inserted, raw_id = await insert_raw_event(raw, db_path=parsed_db_path)
        assert inserted is True
        status = "pending" if raw.source_event_id == "42" else "complete"
        await update_raw_event_status(raw_id, status, None, db_path=parsed_db_path)

    assert await get_latest_telegram_message_id(db_path=parsed_db_path) == 105


@pytest.mark.asyncio
async def test_has_open_and_close(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    station_id = 1
    start = datetime(2026, 3, 18, 5, 45, 0)
    end = start + timedelta(seconds=120)

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999901",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_id = await insert_raw_event(raw, db_path=parsed_db_path)
    assert inserted is True

    msg_open = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.TELEGRAM,
        station_id=station_id,
        station_name="TEST_STATION",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-18",
        report_slot="05:45",
        event_time_local="2026-03-18 05:45:10",
        event_time_utc="2026-03-18T11:45:10+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    await insert_normalized_event(msg_open, db_path=parsed_db_path)
    assert await has_open_and_close(station_id, start, end, db_path=parsed_db_path) is False

    msg_close = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.TELEGRAM,
        station_id=station_id,
        station_name="TEST_STATION",
        station_code=None,
        event_type="CLOSE",
        event_class="STATE",
        report_date="2026-03-18",
        report_slot="05:45",
        event_time_local="2026-03-18 05:45:20",
        event_time_utc="2026-03-18T11:45:20+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    await insert_normalized_event(msg_close, db_path=parsed_db_path)
    assert await has_open_and_close(station_id, start, end, db_path=parsed_db_path) is True


@pytest.mark.asyncio
async def test_has_open_and_close_counts_promoted_paired_close(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    station_id = 1
    start = datetime(2026, 3, 31, 5, 45, 0)
    end = start + timedelta(seconds=120)

    raw_open = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999903",
        raw_payload="TEST OPEN",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_open_id = await insert_raw_event(raw_open, db_path=parsed_db_path)
    assert inserted is True

    msg_open = NormalizedEvent(
        raw_event_id=raw_open_id,
        source=Source.TELEGRAM,
        station_id=station_id,
        station_name="TEST_STATION",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot="05:45",
        event_time_local="2026-03-31 05:46:30",
        event_time_utc="2026-03-31T11:46:30+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        phone_number="5550000001",
        payload_json={},
    )
    await insert_normalized_event(msg_open, db_path=parsed_db_path)
    assert await has_open_and_close(station_id, start, end, db_path=parsed_db_path) is False

    raw_close = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999904",
        raw_payload="TEST CLOSE",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_close_id = await insert_raw_event(raw_close, db_path=parsed_db_path)
    assert inserted is True

    msg_close = NormalizedEvent(
        raw_event_id=raw_close_id,
        source=Source.TELEGRAM,
        station_id=station_id,
        station_name="TEST_STATION",
        station_code=None,
        event_type="CLOSE",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot=None,
        event_time_local="2026-03-31 05:48:03",
        event_time_utc="2026-03-31T11:48:03+00:00",
        tone=False,
        priority=100,
        is_valid_report=False,
        parser_version="telegram:v1.1",
        confidence_score=80,
        phone_number="5550000001",
        payload_json={},
    )
    await insert_normalized_event(msg_close, db_path=parsed_db_path)
    assert await has_open_and_close(station_id, start, end, db_path=parsed_db_path) is True


@pytest.mark.asyncio
async def test_get_events_by_station_local_date_includes_non_tono(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999902",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_id = await insert_raw_event(raw, db_path=parsed_db_path)
    assert inserted is True

    event = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="SINGLE",
        event_class="INFO",
        report_date="2026-03-19",
        report_slot=None,
        event_time_local="2026-03-19 12:00:00",
        event_time_utc="2026-03-19T18:00:00+00:00",
        tone=False,
        priority=100,
        is_valid_report=False,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    await insert_normalized_event(event, db_path=parsed_db_path)

    by_report_date = await get_events_by_station_date(
        "TEST_STATION",
        "2026-03-19",
        db_path=parsed_db_path,
    )
    assert len(by_report_date) == 0

    by_local_date = await get_events_by_station_local_date(
        "TEST_STATION",
        "2026-03-19",
        db_path=parsed_db_path,
    )
    assert len(by_local_date) == 1


@pytest.mark.asyncio
async def test_pruebas_table_captures_off_schedule_tone_zero(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999903",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_id = await insert_raw_event(raw, db_path=parsed_db_path)
    assert inserted is True

    event = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="SINGLE",
        event_class="INFO",
        report_date="2026-03-20",
        report_slot=None,
        event_time_local="2026-03-20 12:00:00",
        event_time_utc="2026-03-20T18:00:00+00:00",
        tone=False,
        priority=100,
        is_valid_report=False,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    await insert_normalized_event(event, db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM pruebas WHERE station_name = ?",
            ("TEST_STATION",),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

        async with db.execute(
            "SELECT COUNT(*) FROM alertas WHERE station_name = ?",
            ("TEST_STATION",),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0


@pytest.mark.asyncio
async def test_alertas_table_captures_eqw_off_schedule(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw = RawEvent(
        source=Source.SERIAL,
        source_event_id="999904",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_id = await insert_raw_event(raw, db_path=parsed_db_path)
    assert inserted is True

    event = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.SERIAL,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="EQW",
        event_class="ALERT",
        report_date="2026-03-21",
        report_slot=None,
        event_time_local="2026-03-21 12:00:00",
        event_time_utc="2026-03-21T18:00:00+00:00",
        tone=False,
        priority=200,
        is_valid_report=False,
        parser_version="serial:v1",
        confidence_score=60,
        payload_json={},
    )
    await insert_normalized_event(event, db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM pruebas WHERE station_name = ?",
            ("TEST_STATION",),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0

        async with db.execute(
            "SELECT COUNT(*) FROM alertas WHERE station_name = ?",
            ("TEST_STATION",),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1


@pytest.mark.asyncio
async def test_first_arrival_valid_for_rwt_slot(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw_early = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999905",
        raw_payload="TEST",
        received_at=datetime(2026, 3, 22, 12, 0, 0),
        transport_meta={},
    )
    raw_late = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="999906",
        raw_payload="TEST",
        received_at=datetime(2026, 3, 22, 12, 5, 0),
        transport_meta={},
    )
    inserted, raw_id_early = await insert_raw_event(raw_early, db_path=parsed_db_path)
    assert inserted is True
    inserted, raw_id_late = await insert_raw_event(raw_late, db_path=parsed_db_path)
    assert inserted is True

    event_early = NormalizedEvent(
        raw_event_id=raw_id_early,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="RWT",
        event_class="TEST",
        report_date="2026-03-22",
        report_slot="05:45",
        event_time_local="2026-03-22 05:45:10",
        event_time_utc="2026-03-22T11:45:10+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    event_late = NormalizedEvent(
        raw_event_id=raw_id_late,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="RWT",
        event_class="TEST",
        report_date="2026-03-22",
        report_slot="05:45",
        event_time_local="2026-03-22 05:45:20",
        event_time_utc="2026-03-22T11:45:20+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    id_early = await insert_normalized_event(event_early, db_path=parsed_db_path)
    id_late = await insert_normalized_event(event_late, db_path=parsed_db_path)

    await enforce_first_valid_for_slot(
        1,
        "2026-03-22",
        "05:45",
        "RWT",
        db_path=parsed_db_path,
    )

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT id, is_valid_report FROM normalized_events WHERE id IN (?, ?) ORDER BY id",
            (id_early, id_late),
        ) as cursor:
            rows = await cursor.fetchall()
            assert rows[0][1] == 1
            assert rows[1][1] == 0


@pytest.mark.asyncio
async def test_state_pairing_stores_payload_metadata(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw_open = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="pair-open",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    raw_close = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="pair-close",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    _, raw_id_open = await insert_raw_event(raw_open, db_path=parsed_db_path)
    _, raw_id_close = await insert_raw_event(raw_close, db_path=parsed_db_path)

    open_event = NormalizedEvent(
        raw_event_id=raw_id_open,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot="05:45",
        event_time_local="2026-03-31 05:46:30",
        event_time_utc="2026-03-31T11:46:30+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    close_event = NormalizedEvent(
        raw_event_id=raw_id_close,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="CLOSE",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot=None,
        event_time_local="2026-03-31 05:48:03",
        event_time_utc="2026-03-31T11:48:03+00:00",
        tone=False,
        priority=100,
        is_valid_report=False,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )

    open_id = await insert_normalized_event(open_event, db_path=parsed_db_path)
    close_id = await insert_normalized_event(close_event, db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            """
            SELECT id, report_slot, tone, is_valid_report, payload_json
            FROM normalized_events
            WHERE id IN (?, ?)
            ORDER BY id
            """,
            (open_id, close_id),
        ) as cursor:
            rows = await cursor.fetchall()

    row_map = {
        row[0]: {
            "report_slot": row[1],
            "tone": row[2],
            "is_valid_report": row[3],
            "payload": json.loads(row[4]),
        }
        for row in rows
    }
    payloads = {event_id: info["payload"] for event_id, info in row_map.items()}
    assert row_map[open_id]["report_slot"] == "05:45"
    assert row_map[open_id]["tone"] == 1
    assert row_map[open_id]["is_valid_report"] == 1
    assert row_map[close_id]["report_slot"] == "05:45"
    assert row_map[close_id]["tone"] == 1
    assert row_map[close_id]["is_valid_report"] == 1
    assert payloads[open_id]["state_pair_event_id"] == close_id
    assert payloads[open_id]["state_pair_event_type"] == "CLOSE"
    assert payloads[open_id]["state_pair_delta_seconds"] == 93
    assert payloads[open_id]["state_pair_direction"] == "after"
    assert payloads[open_id]["state_pair_window_seconds"] == 100
    assert payloads[close_id]["state_pair_event_id"] == open_id
    assert payloads[close_id]["state_pair_event_type"] == "OPEN"
    assert payloads[close_id]["state_pair_delta_seconds"] == 93
    assert payloads[close_id]["state_pair_direction"] == "before"
    assert payloads[close_id]["state_pair_window_seconds"] == 100
    assert payloads[close_id]["state_pair_promoted"] is True
    assert payloads[close_id]["state_pair_original_report_slot"] is None
    assert payloads[close_id]["state_pair_original_tone"] == 0
    assert payloads[close_id]["state_pair_original_is_valid_report"] == 0

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM pruebas WHERE normalized_event_id = ?",
            (close_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0


@pytest.mark.asyncio
async def test_delete_normalized_events_for_raw_clears_pair_metadata(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw_open = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="replace-open",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    raw_close = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="replace-close",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    _, raw_id_open = await insert_raw_event(raw_open, db_path=parsed_db_path)
    _, raw_id_close = await insert_raw_event(raw_close, db_path=parsed_db_path)

    open_event = NormalizedEvent(
        raw_event_id=raw_id_open,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot="08:45",
        event_time_local="2026-03-31 08:45:39",
        event_time_utc="2026-03-31T14:45:39+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    close_event = NormalizedEvent(
        raw_event_id=raw_id_close,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="CLOSE",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot="08:45",
        event_time_local="2026-03-31 08:45:24",
        event_time_utc="2026-03-31T14:45:24+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )

    open_id = await insert_normalized_event(open_event, db_path=parsed_db_path)
    close_id = await insert_normalized_event(close_event, db_path=parsed_db_path)
    await delete_normalized_events_for_raw(raw_id_close, db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT payload_json FROM normalized_events WHERE id = ?",
            (open_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            payload = json.loads(row[0])
            assert "state_pair_event_id" not in payload

        async with db.execute(
            "SELECT COUNT(*) FROM normalized_events WHERE id = ?",
            (close_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0


@pytest.mark.asyncio
async def test_delete_normalized_events_for_raw_reverts_promoted_pair_fields(parsed_db_path):
    await init_db(db_path=parsed_db_path)

    raw_open = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="promote-open",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    raw_close = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="promote-close",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    _, raw_id_open = await insert_raw_event(raw_open, db_path=parsed_db_path)
    _, raw_id_close = await insert_raw_event(raw_close, db_path=parsed_db_path)

    open_event = NormalizedEvent(
        raw_event_id=raw_id_open,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot="05:45",
        event_time_local="2026-03-31 05:46:30",
        event_time_utc="2026-03-31T11:46:30+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    close_event = NormalizedEvent(
        raw_event_id=raw_id_close,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="CLOSE",
        event_class="STATE",
        report_date="2026-03-31",
        report_slot=None,
        event_time_local="2026-03-31 05:48:03",
        event_time_utc="2026-03-31T11:48:03+00:00",
        tone=False,
        priority=100,
        is_valid_report=False,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )

    open_id = await insert_normalized_event(open_event, db_path=parsed_db_path)
    close_id = await insert_normalized_event(close_event, db_path=parsed_db_path)
    await delete_normalized_events_for_raw(raw_id_open, db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            """
            SELECT report_slot, tone, is_valid_report, payload_json
            FROM normalized_events
            WHERE id = ?
            """,
            (close_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] is None
            assert row[1] == 0
            assert row[2] == 0
            payload = json.loads(row[3])
            assert "state_pair_event_id" not in payload
            assert "state_pair_promoted" not in payload

        async with db.execute(
            "SELECT COUNT(*) FROM pruebas WHERE normalized_event_id = ?",
            (close_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

        async with db.execute(
            "SELECT COUNT(*) FROM normalized_events WHERE id = ?",
            (open_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0


@pytest.mark.asyncio
async def test_init_db_migrates_raw_events_to_source_specific_dedupe(parsed_db_path):
    with sqlite3.connect(str(parsed_db_path)) as conn:
        _create_raw_events_table(conn, legacy_unique=True)
        conn.commit()

    await init_db(db_path=parsed_db_path)

    await _assert_source_specific_dedupe(parsed_db_path)


@pytest.mark.asyncio
async def test_init_db_repairs_legacy_raw_events_with_incoming_messages_view(parsed_db_path):
    with sqlite3.connect(str(parsed_db_path)) as conn:
        _create_raw_events_table(conn, legacy_unique=True)
        _insert_raw_event_row(
            conn,
            table_name="raw_events",
            source="manual",
            source_event_id="legacy-1",
            raw_payload="LEGACY",
            raw_hash="legacy-hash",
        )
        conn.execute("CREATE VIEW incoming_messages AS SELECT * FROM raw_events")
        conn.commit()

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM raw_events WHERE source = ?",
            (Source.SERIAL.value,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0

    await init_db(db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute("SELECT COUNT(*) FROM incoming_messages") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_raw_telegram_event'"
        ) as cursor:
            assert await cursor.fetchone() is not None


@pytest.mark.asyncio
async def test_init_db_discards_stale_raw_events_new_before_retrying(parsed_db_path):
    with sqlite3.connect(str(parsed_db_path)) as conn:
        _create_raw_events_table(conn, legacy_unique=True)
        _insert_raw_event_row(
            conn,
            table_name="raw_events",
            source="manual",
            source_event_id="live-1",
            raw_payload="LIVE",
            raw_hash="live-hash",
        )
        _create_raw_events_table(conn, table_name="raw_events_new")
        _insert_raw_event_row(
            conn,
            table_name="raw_events_new",
            source="stale",
            source_event_id="stale-1",
            raw_payload="STALE",
            raw_hash="stale-hash",
        )
        conn.execute("CREATE VIEW incoming_messages AS SELECT * FROM raw_events")
        conn.commit()

    await init_db(db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'raw_events_new'"
        ) as cursor:
            assert await cursor.fetchone() is None

        async with db.execute("SELECT COUNT(*) FROM raw_events WHERE source = 'manual'") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1

    await _assert_source_specific_dedupe(parsed_db_path)


@pytest.mark.asyncio
async def test_init_db_salvages_raw_events_new_without_raw_events(parsed_db_path):
    with sqlite3.connect(str(parsed_db_path)) as conn:
        _create_raw_events_table(conn, table_name="raw_events_new")
        _insert_raw_event_row(
            conn,
            table_name="raw_events_new",
            source="manual",
            source_event_id="temp-1",
            raw_payload="SALVAGE",
            raw_hash="salvage-hash",
        )
        conn.execute("CREATE VIEW incoming_messages AS SELECT * FROM raw_events")
        conn.commit()

    await init_db(db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'raw_events'"
        ) as cursor:
            assert await cursor.fetchone() is not None

        async with db.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'raw_events_new'"
        ) as cursor:
            assert await cursor.fetchone() is None

        async with db.execute("SELECT COUNT(*) FROM incoming_messages") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1
