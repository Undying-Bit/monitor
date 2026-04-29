from __future__ import annotations

from datetime import datetime

import pytest

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


def test_normalize_telegram_ambiguous_phone_without_hint_stores_null_station(make_station_db):
    station_db = make_station_db(
        [
            (1, "ALFA", "5511111111", "ALFA OPEN", "ALFA CLOSE", 0, "RED-A"),
            (2, "BETA", "5511111111", "BETA OPEN", "BETA CLOSE", 0, "RED-B"),
        ]
    )
    station_manager = StationManager(db_path=station_db)
    resolver = StationResolver(station_manager)

    parsed = parse_telegram("+525511111111 31/03/2026 05:45:10 ALFA OPEN")
    assert parsed is not None

    event = normalize_telegram(
        parsed,
        raw_event_id=1,
        station_manager=station_manager,
        resolver=resolver,
    )

    assert event.station_id is None
    assert event.is_valid_report is False


def test_normalize_telegram_ambiguous_phone_uses_legacy_hint_alias(make_station_db):
    station_db = make_station_db(
        [
            (4, "Cuajimalpa", "2221823401", "", "", 0, "CDMX"),
            (5, "Teuhtli", "2221823401", "", "", 0, "CDMX"),
            (6, "Zacatenco", "2221823401", "", "", 0, "CDMX"),
        ]
    )
    station_manager = StationManager(db_path=station_db)
    resolver = StationResolver(station_manager)

    parsed = parse_telegram(
        "+522221823401 22/04/2026 11:45:26 MENSAJE **/07/21 11:45:26 prueba TEUTLI canal 3"
    )
    assert parsed is not None

    event = normalize_telegram(
        parsed,
        raw_event_id=1,
        station_manager=station_manager,
        resolver=resolver,
    )

    assert event.station_id == 5
    assert event.station_name == "Teuhtli"
    assert event.channel == "3"
    assert event.is_valid_report is True


def test_normalize_serial_rejects_invalid_time_fields(make_station_db):
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
        julian_day=400,
        hour=25,
        minute=61,
        transmitter_code="XCMX/011",
        raw_header="ZCZC-WXR-RWT-001001+0030-4002561-XCMX/011-",
    )

    with pytest.raises(ValueError, match="serial_invalid_"):
        normalize_serial(
            parsed,
            raw_event_id=1,
            resolver=resolver,
            received_at=datetime(2026, 3, 31, 18, 0, 0),
        )


def test_normalize_serial_transmitter_map_resolves_renamed_teuhtli(make_station_db):
    station_db = make_station_db(
        [
            (5, "Teuhtli", "2221823401", "", "", 0, "CDMX"),
        ]
    )
    station_manager = StationManager(db_path=station_db)
    resolver = StationResolver(station_manager)

    parsed = SerialParsed(
        originator="WXR",
        event_code="RWT",
        area_codes=["001001"],
        duration_code="0030",
        julian_day=112,
        hour=17,
        minute=45,
        transmitter_code="XCMX/011",
        raw_header="ZCZC-WXR-RWT-001001+0030-1121745-XCMX/011-",
    )

    event = normalize_serial(
        parsed,
        raw_event_id=1,
        resolver=resolver,
        received_at=datetime(2026, 4, 22, 18, 0, 0),
    )

    assert event.station_id == 5
    assert event.station_name == "Teuhtli"
