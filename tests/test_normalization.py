from __future__ import annotations

from datetime import datetime

from models import SerialParsed
from normalization import normalize_serial, normalize_telegram
from parser_engine import parse_telegram
from station_manager import StationManager
from station_resolver import StationResolver


def test_normalize_telegram_off_schedule_keeps_calendar_report_date(make_station_db):
    station_db = make_station_db(
        [
            (1, "PC Colima", "5561048913", "COLIMA SUP", "Restablecimiento", 0, "Colima"),
        ]
    )
    station_manager = StationManager(db_path=station_db)
    resolver = StationResolver(station_manager)

    parsed = parse_telegram("+525561048913 31/03/2026 3:46:06 Restablecimiento")
    assert parsed is not None

    event = normalize_telegram(
        parsed,
        raw_event_id=1,
        station_manager=station_manager,
        resolver=resolver,
    )

    assert event.report_date == "2026-03-31"
    assert event.report_slot is None
    assert event.tone is False
    assert event.is_valid_report is False
    assert event.event_type == "CLOSE"


def test_normalize_serial_off_schedule_keeps_calendar_report_date(make_station_db):
    station_db = make_station_db(
        [
            (1, "TEUTLI", "5550000000", "", "", 0, "CDMX"),
        ]
    )
    station_manager = StationManager(db_path=station_db)
    resolver = StationResolver(station_manager)

    parsed = SerialParsed(
        originator="WXR",
        event_code="RWT",
        area_codes=["001001"],
        duration_code="0030",
        julian_day=90,
        hour=12,
        minute=0,
        transmitter_code="XCMX/011",
        raw_header="ZCZC-WXR-RWT-001001+0030-0901200-XCMX/011-",
    )

    event = normalize_serial(
        parsed,
        raw_event_id=1,
        resolver=resolver,
        received_at=datetime(2026, 3, 31, 18, 0, 0),
    )

    assert event.report_date == "2026-03-31"
    assert event.report_slot is None
    assert event.tone is False
    assert event.is_valid_report is False
