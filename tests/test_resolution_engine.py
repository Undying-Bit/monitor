import sys
import os
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models import NormalizedEvent, RawEvent, Source
from persistence import init_db, insert_normalized_event, insert_raw_event, open_db
from resolution_engine import resolve_slot


@pytest.mark.asyncio
async def test_resolve_slot_prefers_open_when_open_and_close_are_valid(parsed_db_path):
    await init_db(db_path=parsed_db_path)
    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="r1",
        raw_payload="TEST",
        received_at=datetime.utcnow(),
        transport_meta={},
    )
    inserted, raw_id = await insert_raw_event(raw, db_path=parsed_db_path)
    assert inserted is True

    e1 = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-18",
        report_slot="05:45",
        event_time_local="2026-03-18 05:45:30",
        event_time_utc="2026-03-18T11:45:30+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )
    e2 = NormalizedEvent(
        raw_event_id=raw_id,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST_STATION",
        station_code=None,
        event_type="CLOSE",
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
    id1 = await insert_normalized_event(e1, db_path=parsed_db_path)
    id2 = await insert_normalized_event(e2, db_path=parsed_db_path)

    await resolve_slot(1, "2026-03-18", "05:45", db_path=parsed_db_path)

    async with open_db(parsed_db_path) as db:
        async with db.execute(
            """
            SELECT effective_event_id FROM resolved_report_slots
            WHERE station_id = ? AND report_date = ? AND report_slot = ?
            """,
            (1, "2026-03-18", "05:45"),
        ) as cursor:
            row = await cursor.fetchone()
            assert row is not None
            assert row[0] == id1
