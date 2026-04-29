from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path

import rename_station
from models import NormalizedEvent, RawEvent, Source
from persistence import init_db, insert_normalized_event, insert_raw_event
from resolution_engine import resolve_slot


def _create_reportes_db(db_path: Path, rows: list[tuple[str, str, int, int, str | None, float, str]]) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE reportes (
                id INTEGER PRIMARY KEY,
                estacion TEXT NOT NULL,
                fecha TEXT NOT NULL,
                consecutivos_perdidos INTEGER NOT NULL,
                pruebas INTEGER NOT NULL,
                prueba_time TEXT,
                disponibilidad REAL NOT NULL,
                reportes TEXT NOT NULL,
                UNIQUE(estacion, fecha)
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO reportes (
                estacion,
                fecha,
                consecutivos_perdidos,
                pruebas,
                prueba_time,
                disponibilidad,
                reportes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


def _create_sync_db(
    db_path: Path,
    prune_rows: list[tuple[str, str, str, int]] | None = None,
) -> None:
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE prune_candidates (
                station TEXT PRIMARY KEY,
                first_missing_at TEXT NOT NULL,
                last_missing_at TEXT NOT NULL,
                missing_scan_count INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        if prune_rows:
            conn.executemany(
                """
                INSERT INTO prune_candidates (
                    station,
                    first_missing_at,
                    last_missing_at,
                    missing_scan_count
                )
                VALUES (?, ?, ?, ?)
                """,
                prune_rows,
            )
        conn.commit()


async def _insert_raw(db_path: Path, *, source: Source, source_event_id: str) -> int:
    raw = RawEvent(
        source=source,
        source_event_id=source_event_id,
        raw_payload=f"RAW-{source_event_id}",
        received_at=datetime(2026, 4, 21, 12, 0, 0),
        transport_meta={},
    )
    inserted, raw_id = await insert_raw_event(raw, db_path=db_path)
    assert inserted is True
    return raw_id


async def _seed_monitor_data(db_path: Path, *, station_id: int, station_name: str) -> None:
    await init_db(db_path=db_path)

    raw_rwt_id = await _insert_raw(db_path, source=Source.TELEGRAM, source_event_id="tg-rwt")
    await insert_normalized_event(
        NormalizedEvent(
            raw_event_id=raw_rwt_id,
            source=Source.TELEGRAM,
            station_id=station_id,
            station_name=station_name,
            station_code=None,
            event_type="RWT",
            event_class="TEST",
            report_date="2026-04-20",
            report_slot="05:45",
            event_time_local="2026-04-20 05:45:10",
            event_time_utc="2026-04-20T11:45:10+00:00",
            tone=True,
            priority=100,
            is_valid_report=True,
            parser_version="telegram:v1.1",
            confidence_score=85,
            payload_json={},
        ),
        db_path=db_path,
    )
    await resolve_slot(station_id, "2026-04-20", "05:45", db_path=db_path)

    raw_prueba_id = await _insert_raw(db_path, source=Source.TELEGRAM, source_event_id="tg-prueba")
    await insert_normalized_event(
        NormalizedEvent(
            raw_event_id=raw_prueba_id,
            source=Source.TELEGRAM,
            station_id=station_id,
            station_name=station_name,
            station_code=None,
            event_type="SINGLE",
            event_class="INFO",
            report_date="2026-04-20",
            report_slot=None,
            event_time_local="2026-04-20 12:00:00",
            event_time_utc="2026-04-20T18:00:00+00:00",
            tone=False,
            priority=100,
            is_valid_report=False,
            parser_version="telegram:v1.1",
            confidence_score=80,
            payload_json={},
        ),
        db_path=db_path,
    )

    raw_alerta_id = await _insert_raw(db_path, source=Source.SERIAL, source_event_id="serial-alerta")
    await insert_normalized_event(
        NormalizedEvent(
            raw_event_id=raw_alerta_id,
            source=Source.SERIAL,
            station_id=station_id,
            station_name=station_name,
            station_code=None,
            event_type="EQW",
            event_class="ALERT",
            report_date="2026-04-20",
            report_slot=None,
            event_time_local="2026-04-20 13:00:00",
            event_time_utc="2026-04-20T19:00:00+00:00",
            tone=False,
            priority=200,
            is_valid_report=False,
            parser_version="serial:v1",
            confidence_score=60,
            payload_json={},
        ),
        db_path=db_path,
    )


def _build_args(
    *,
    current_name: str,
    new_name: str,
    stations_db: Path,
    mensajes_db: Path,
    reportes_db: Path,
    sync_db: Path,
    dry_run: bool = False,
) -> list[str]:
    args = [
        "--current-name",
        current_name,
        "--new-name",
        new_name,
        "--stations-db",
        str(stations_db),
        "--mensajes-db",
        str(mensajes_db),
        "--reportes-db",
        str(reportes_db),
        "--sync-db",
        str(sync_db),
    ]
    if dry_run:
        args.append("--dry-run")
    else:
        args.append("--yes")
    return args


def test_successful_rename_updates_all_databases(workspace_tmp_dir, make_station_db, parsed_db_path):
    stations_db = make_station_db(
        [
            (1, "Old Station", "+525511111111", "OPEN", "CLOSE", 0, "RED-A"),
            (2, "Other Station", "+525522222222", "OPEN", "CLOSE", 0, "RED-B"),
        ]
    )
    asyncio.run(_seed_monitor_data(parsed_db_path, station_id=1, station_name="Old Station"))

    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(
        reportes_db,
        [("OLD STATION", "2026-04-20", 0, 1, "05:45", 99.0, "X-------")],
    )

    sync_db = workspace_tmp_dir / "sync_tracking.db"
    _create_sync_db(
        sync_db,
        [
            ("OLD STATION", "2026-04-18T00:00:00", "2026-04-19T00:00:00", 2),
            ("NEW STATION", "2026-04-20T00:00:00", "2026-04-21T00:00:00", 5),
        ],
    )

    exit_code = rename_station.main(
        _build_args(
            current_name="Old Station",
            new_name="New Station",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 0

    with sqlite3.connect(str(stations_db)) as conn:
        row = conn.execute(
            "SELECT nombre FROM Estaciones WHERE station_id = ?",
            (1,),
        ).fetchone()
        assert row == ("New Station",)

    with sqlite3.connect(str(parsed_db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM normalized_events WHERE station_id = ? AND station_name = ?",
            (1, "New Station"),
        ).fetchone()[0] == 3
        assert conn.execute(
            "SELECT COUNT(*) FROM pruebas WHERE station_id = ? AND station_name = ?",
            (1, "New Station"),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM alertas WHERE station_id = ? AND station_name = ?",
            (1, "New Station"),
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM resolved_report_slots WHERE station_id = ? AND station_name = ?",
            (1, "New Station"),
        ).fetchone()[0] == 1

    with sqlite3.connect(str(reportes_db)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM reportes WHERE estacion = ?",
            ("NEW STATION",),
        ).fetchone()[0] == 1

    with sqlite3.connect(str(sync_db)) as conn:
        rows = conn.execute(
            """
            SELECT station, first_missing_at, last_missing_at, missing_scan_count
            FROM prune_candidates
            ORDER BY station
            """
        ).fetchall()
        assert rows == [("NEW STATION", "2026-04-18T00:00:00", "2026-04-21T00:00:00", 7)]

    assert list(workspace_tmp_dir.glob("estaciones.backup-*.db"))
    assert list(workspace_tmp_dir.glob("mensajes.backup-*.db"))
    assert list(workspace_tmp_dir.glob("reportes.backup-*.db"))
    assert list(workspace_tmp_dir.glob("sync_tracking.backup-*.db"))


def test_rename_fails_when_current_station_missing(workspace_tmp_dir, make_station_db, parsed_db_path, capsys):
    stations_db = make_station_db([(1, "Alpha", "+525511111111", "OPEN", "CLOSE", 0, "RED-A")])
    asyncio.run(_seed_monitor_data(parsed_db_path, station_id=1, station_name="Alpha"))
    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(reportes_db, [])
    sync_db = workspace_tmp_dir / "sync_tracking.db"
    _create_sync_db(sync_db)

    exit_code = rename_station.main(
        _build_args(
            current_name="Missing",
            new_name="Renamed",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 1
    assert "Current station name not found" in capsys.readouterr().err

    with sqlite3.connect(str(stations_db)) as conn:
        assert conn.execute("SELECT nombre FROM Estaciones WHERE station_id = 1").fetchone()[0] == "Alpha"


def test_rename_fails_when_current_station_is_ambiguous(workspace_tmp_dir, make_station_db, parsed_db_path, capsys):
    stations_db = make_station_db(
        [
            (1, "Alpha", "+525511111111", "OPEN", "CLOSE", 0, "RED-A"),
            (2, " alpha ", "+525522222222", "OPEN", "CLOSE", 0, "RED-B"),
        ]
    )
    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(reportes_db, [])
    sync_db = workspace_tmp_dir / "sync_tracking.db"
    _create_sync_db(sync_db)

    exit_code = rename_station.main(
        _build_args(
            current_name="ALPHA",
            new_name="Beta",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 1
    assert "ambiguous" in capsys.readouterr().err.lower()


def test_rename_fails_when_new_name_already_exists(workspace_tmp_dir, make_station_db, parsed_db_path, capsys):
    stations_db = make_station_db(
        [
            (1, "Alpha", "+525511111111", "OPEN", "CLOSE", 0, "RED-A"),
            (2, "Beta", "+525522222222", "OPEN", "CLOSE", 0, "RED-B"),
        ]
    )
    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(reportes_db, [])
    sync_db = workspace_tmp_dir / "sync_tracking.db"
    _create_sync_db(sync_db)

    exit_code = rename_station.main(
        _build_args(
            current_name="Alpha",
            new_name=" beta ",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 1
    assert "already exists" in capsys.readouterr().err


def test_rename_merges_when_reportes_key_would_collide(workspace_tmp_dir, make_station_db, parsed_db_path):
    stations_db = make_station_db([(1, "Old Station", "+525511111111", "OPEN", "CLOSE", 0, "RED-A")])
    asyncio.run(_seed_monitor_data(parsed_db_path, station_id=1, station_name="Old Station"))

    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(
        reportes_db,
        [
            ("OLD STATION", "2026-04-20", 0, 0, None, 0.0, "XXXXXXXX"),
            ("NEW STATION", "2026-04-20", 0, 1, "05:45", 98.0, "O-------"),
            ("OLD STATION", "2026-04-21", 0, 0, None, 0.0, "XXXX----"),
        ],
    )
    sync_db = workspace_tmp_dir / "sync_tracking.db"
    _create_sync_db(sync_db)

    exit_code = rename_station.main(
        _build_args(
            current_name="Old Station",
            new_name="New Station",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 0

    with sqlite3.connect(str(stations_db)) as conn:
        assert conn.execute("SELECT nombre FROM Estaciones WHERE station_id = 1").fetchone()[0] == "New Station"
    with sqlite3.connect(str(reportes_db)) as conn:
        rows = conn.execute(
            """
            SELECT estacion, fecha, pruebas, prueba_time, disponibilidad, reportes
            FROM reportes
            ORDER BY fecha
            """
        ).fetchall()
        assert rows == [
            ("NEW STATION", "2026-04-20", 1, "05:45", 98.0, "O-------"),
            ("NEW STATION", "2026-04-21", 0, None, 0.0, "XXXX----"),
        ]


def test_rename_fails_when_reportes_current_key_already_has_duplicates(
    workspace_tmp_dir,
    make_station_db,
    parsed_db_path,
    capsys,
):
    stations_db = make_station_db([(1, "Old Station", "+525511111111", "OPEN", "CLOSE", 0, "RED-A")])
    asyncio.run(_seed_monitor_data(parsed_db_path, station_id=1, station_name="Old Station"))

    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(
        reportes_db,
        [
            ("OLD STATION", "2026-04-20", 0, 1, None, 99.0, "X-------"),
            (" old station ", "2026-04-20", 0, 1, None, 98.0, "O-------"),
        ],
    )
    sync_db = workspace_tmp_dir / "sync_tracking.db"
    _create_sync_db(sync_db)

    exit_code = rename_station.main(
        _build_args(
            current_name="Old Station",
            new_name="New Station",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 1
    captured = capsys.readouterr()
    assert "duplicate normalized rows for the current station" in captured.err


def test_display_only_rename_keeps_reportes_key_unchanged(workspace_tmp_dir, make_station_db, parsed_db_path):
    stations_db = make_station_db([(1, "PC Colima", "+525511111111", "OPEN", "CLOSE", 0, "RED-A")])
    asyncio.run(_seed_monitor_data(parsed_db_path, station_id=1, station_name="PC Colima"))

    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(
        reportes_db,
        [("PC COLIMA", "2026-04-20", 0, 1, None, 99.0, "X-------")],
    )
    sync_db = workspace_tmp_dir / "missing_sync.db"

    exit_code = rename_station.main(
        _build_args(
            current_name="PC Colima",
            new_name="pc colima",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
        )
    )

    assert exit_code == 0

    with sqlite3.connect(str(stations_db)) as conn:
        assert conn.execute("SELECT nombre FROM Estaciones WHERE station_id = 1").fetchone()[0] == "pc colima"
    with sqlite3.connect(str(parsed_db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM normalized_events WHERE station_id = ? AND station_name = ?",
            (1, "pc colima"),
        ).fetchone()[0] == 3
    with sqlite3.connect(str(reportes_db)) as conn:
        assert conn.execute(
            "SELECT estacion FROM reportes WHERE fecha = ?",
            ("2026-04-20",),
        ).fetchone()[0] == "PC COLIMA"


def test_dry_run_does_not_write_changes(workspace_tmp_dir, make_station_db, parsed_db_path, capsys):
    stations_db = make_station_db([(1, "Alpha", "+525511111111", "OPEN", "CLOSE", 0, "RED-A")])
    asyncio.run(_seed_monitor_data(parsed_db_path, station_id=1, station_name="Alpha"))

    reportes_db = workspace_tmp_dir / "reportes.db"
    _create_reportes_db(
        reportes_db,
        [("ALPHA", "2026-04-20", 0, 1, None, 99.0, "X-------")],
    )
    sync_db = workspace_tmp_dir / "missing_sync.db"

    exit_code = rename_station.main(
        _build_args(
            current_name="Alpha",
            new_name="Bravo",
            stations_db=stations_db,
            mensajes_db=parsed_db_path,
            reportes_db=reportes_db,
            sync_db=sync_db,
            dry_run=True,
        )
    )

    assert exit_code == 0
    assert "Dry run complete" in capsys.readouterr().out

    with sqlite3.connect(str(stations_db)) as conn:
        assert conn.execute("SELECT nombre FROM Estaciones WHERE station_id = 1").fetchone()[0] == "Alpha"
    with sqlite3.connect(str(parsed_db_path)) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM normalized_events WHERE station_name = ?",
            ("Alpha",),
        ).fetchone()[0] == 3
    with sqlite3.connect(str(reportes_db)) as conn:
        assert conn.execute("SELECT estacion FROM reportes").fetchone()[0] == "ALPHA"
    assert not list(workspace_tmp_dir.glob("*.backup-*.db"))
