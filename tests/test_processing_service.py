from __future__ import annotations

import aiosqlite
import sqlite3
from datetime import datetime

import pytest

import persistence
import processing_service
from models import RawEvent, Source
from persistence import init_db, insert_raw_event, open_db
from processing_service import ProcessingService
from station_manager import StationManager


@pytest.mark.asyncio
async def test_processing_service_preserves_raw_event_on_mid_pipeline_failure(
    parsed_db_path,
    make_station_db,
    monkeypatch,
):
    station_db = make_station_db(
        [
            (1, "ALFA", "5511111111", "ALFA OPEN", "ALFA CLOSE", 0, "RED-A"),
        ]
    )
    await init_db(db_path=parsed_db_path)
    processor = ProcessingService(StationManager(db_path=station_db), db_path=parsed_db_path)
    await processor.start()

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="101",
        raw_payload="+525511111111 31/03/2026 05:45:10 ALFA OPEN",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    async def _raise(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(processing_service.persistence, "insert_normalized_event", _raise)

    result = await processor.process_new_raw(raw)

    await processor.close()

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT parse_status, parse_error FROM raw_events"
        ) as cursor:
            row = await cursor.fetchone()
            assert row == ("error", "processing_failure:RuntimeError")
        async with db.execute("SELECT COUNT(*) FROM normalized_events") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 0

    assert result.status == "error"
    assert result.normalized_count == 0


@pytest.mark.asyncio
async def test_processing_service_reprocess_matches_live_result(
    workspace_tmp_dir,
    make_station_db,
):
    station_db = make_station_db(
        [
            (1, "ALFA", "5511111111", "ALFA OPEN", "ALFA CLOSE", 0, "RED-A"),
        ]
    )
    live_db = workspace_tmp_dir / "live.db"
    replay_db = workspace_tmp_dir / "replay.db"

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="102",
        raw_payload="+525511111111 31/03/2026 05:45:10 ALFA OPEN",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    await init_db(db_path=live_db)
    live_processor = ProcessingService(StationManager(db_path=station_db), db_path=live_db)
    await live_processor.start()
    live_result = await live_processor.process_new_raw(raw)
    await live_processor.close()

    await init_db(db_path=replay_db)
    inserted, raw_id = await insert_raw_event(raw, db_path=replay_db)
    assert inserted is True
    replay_processor = ProcessingService(StationManager(db_path=station_db), db_path=replay_db)
    await replay_processor.start()
    replay_result = await replay_processor.reprocess_existing_raw(raw_id, raw, replace=True)
    await replay_processor.close()

    assert live_result.status == replay_result.status == "complete"
    assert live_result.normalized_count == replay_result.normalized_count == 1

    async with open_db(live_db) as live_conn, open_db(replay_db) as replay_conn:
        live_row = await (await live_conn.execute(
            """
            SELECT source, station_id, report_date, report_slot, event_type, event_class,
                   tone, is_valid_report, phone_number
            FROM normalized_events
            """
        )).fetchone()
        replay_row = await (await replay_conn.execute(
            """
            SELECT source, station_id, report_date, report_slot, event_type, event_class,
                   tone, is_valid_report, phone_number
            FROM normalized_events
            """
        )).fetchone()
        assert live_row == replay_row


def test_make_raw_event_tolerates_corrupted_transport_metadata(caplog):
    from reprocess_raw_events import _make_raw_event

    row = (
        77,
        "telegram",
        "103",
        "2026-03-31T05:45:10",
        "+525511111111 31/03/2026 05:45:10 ALFA OPEN",
        "{bad json",
    )

    raw_event_id, raw = _make_raw_event(row)

    assert raw_event_id == 77
    assert raw.transport_meta == {}
    assert "Corrupted transport_meta_json" in caplog.text


@pytest.mark.asyncio
async def test_processing_service_reprocess_failure_updates_existing_raw_without_partial_delete(
    parsed_db_path,
    make_station_db,
    monkeypatch,
):
    station_db = make_station_db(
        [
            (1, "ALFA", "5511111111", "ALFA OPEN", "ALFA CLOSE", 0, "RED-A"),
        ]
    )
    await init_db(db_path=parsed_db_path)
    processor = ProcessingService(StationManager(db_path=station_db), db_path=parsed_db_path)
    await processor.start()

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="104",
        raw_payload="+525511111111 31/03/2026 05:45:10 ALFA OPEN",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    live_result = await processor.process_new_raw(raw)
    assert live_result.status == "complete"

    async def _raise(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        processing_service.persistence,
        "delete_normalized_events_for_raw",
        _raise,
    )

    replay_result = await processor.reprocess_existing_raw(
        live_result.raw_event_id,
        raw,
        replace=True,
    )

    await processor.close()

    assert replay_result.status == "error"
    assert replay_result.raw_event_id == live_result.raw_event_id
    assert replay_result.normalized_count == 0

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            "SELECT parse_status, parse_error FROM raw_events WHERE id = ?",
            (live_result.raw_event_id,),
        ) as cursor:
            row = await cursor.fetchone()
            assert row == ("error", "processing_failure:RuntimeError")
        async with db.execute("SELECT COUNT(*) FROM normalized_events") as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1


@pytest.mark.asyncio
async def test_retry_on_locked_does_not_retry_when_shared_connection_is_supplied(
    parsed_db_path,
    monkeypatch,
):
    await init_db(db_path=parsed_db_path)
    calls = 0

    async def _unexpected_sleep(*args, **kwargs):
        raise AssertionError("shared-connection retries should not sleep")

    monkeypatch.setattr(persistence.asyncio, "sleep", _unexpected_sleep)

    @persistence.retry_on_locked(max_retries=3, base_delay=0.01)
    async def _always_locked(*, db=None):
        nonlocal calls
        calls += 1
        raise aiosqlite.OperationalError("database is locked")

    async with open_db(parsed_db_path) as db:
        with pytest.raises(aiosqlite.OperationalError, match="locked"):
            await _always_locked(db=db)

    assert calls == 1
