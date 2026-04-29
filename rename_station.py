"""
rename_station.py - Rename a station across Monitor and Reportes SQLite stores.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from config import PARSED_DB, STATIONS_DB


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_REPORTES_DB = BASE_DIR.parent / "reportes" / "databases" / "reportes.db"
DEFAULT_SYNC_DB = BASE_DIR.parent / "reportes" / "databases" / "sync_tracking.db"
SQLITE_BUSY_TIMEOUT_MS = 1_000


class RenameError(RuntimeError):
    """User-facing rename failure."""


@dataclass(frozen=True)
class RenamePaths:
    stations_db: Path
    mensajes_db: Path
    reportes_db: Path
    sync_db: Path


@dataclass(frozen=True)
class StationSelection:
    station_id: int
    current_name: str
    new_name: str


@dataclass(frozen=True)
class RenamePreview:
    station: StationSelection
    paths: RenamePaths
    report_old_key: str
    report_new_key: str
    normalized_events_count: int
    pruebas_count: int
    alertas_count: int
    resolved_slots_count: int
    reportes_count: int
    sync_available: bool
    sync_has_prune_candidates: bool
    sync_old_count: int
    sync_new_count: int
    report_collision_dates: tuple[str, ...]
    report_duplicate_old_dates: tuple[str, ...]

    @property
    def report_key_changes(self) -> bool:
        return self.report_old_key != self.report_new_key

    @property
    def has_report_conflicts(self) -> bool:
        return bool(self.report_collision_dates or self.report_duplicate_old_dates)


def _clean_display_name(value: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise RenameError("Station names cannot be empty.")
    return cleaned


def _normalize_station_name(value: str) -> str:
    return _clean_display_name(value).casefold()


def _normalize_existing_station_name(value: object | None) -> str:
    if value is None:
        return ""
    return str(value).strip().casefold()


def _normalize_report_station(value: str) -> str:
    return " ".join(_clean_display_name(value).split()).upper()


def _sqlite_error(db_path: Path, exc: sqlite3.Error) -> RenameError:
    message = str(exc)
    if "locked" in message.lower() or "busy" in message.lower():
        return RenameError(
            f"Database is locked or busy: {db_path}. Stop Monitor and Reportes before running the rename."
        )
    return RenameError(f"SQLite error for {db_path}: {message}")


def _open_connection(db_path: Path, *, must_exist: bool = True) -> sqlite3.Connection | None:
    resolved = db_path.resolve()
    if must_exist and not resolved.exists():
        raise RenameError(f"Database not found: {resolved}")
    if not must_exist and not resolved.exists():
        return None
    try:
        conn = sqlite3.connect(str(resolved), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    except sqlite3.Error as exc:
        raise _sqlite_error(resolved, exc) from exc
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {SQLITE_BUSY_TIMEOUT_MS}")
    return conn


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = ?
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _require_tables(conn: sqlite3.Connection, db_path: Path, tables: Sequence[str]) -> None:
    missing = [table for table in tables if not _table_exists(conn, table)]
    if missing:
        raise RenameError(
            f"Database {db_path.resolve()} is missing required tables: {', '.join(sorted(missing))}"
        )


def _count_rows(conn: sqlite3.Connection, sql: str, params: Sequence[object]) -> int:
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None:
        return 0
    return int(row[0])


def _collect_dates(conn: sqlite3.Connection, sql: str, params: Sequence[object]) -> tuple[str, ...]:
    return tuple(str(row[0]) for row in conn.execute(sql, tuple(params)).fetchall())


def _reportes_quality(row: sqlite3.Row) -> tuple[int, int, int, float]:
    reportes = str(row["reportes"] or "")
    resolved_slots = sum(1 for char in reportes if char in {"O", "X"})
    pruebas = int(row["pruebas"] or 0)
    has_prueba_time = 1 if row["prueba_time"] else 0
    disponibilidad = float(row["disponibilidad"] or 0.0)
    return (int(disponibilidad * 1000), pruebas, has_prueba_time, resolved_slots)


def _resolve_station(
    conn: sqlite3.Connection,
    *,
    current_name: str,
    new_name: str,
) -> StationSelection:
    current_norm = _normalize_station_name(current_name)
    new_display_name = _clean_display_name(new_name)
    new_norm = _normalize_station_name(new_display_name)

    rows = conn.execute("SELECT station_id, nombre FROM Estaciones ORDER BY station_id ASC").fetchall()
    matches = [
        row
        for row in rows
        if _normalize_existing_station_name(row["nombre"]) == current_norm
    ]

    if not matches:
        raise RenameError(f"Current station name not found: {current_name!r}")
    if len(matches) > 1:
        candidates = ", ".join(f"{int(row['station_id'])}:{row['nombre']}" for row in matches)
        raise RenameError(f"Current station name is ambiguous: {current_name!r}. Matches: {candidates}")

    selected = matches[0]
    station_id = int(selected["station_id"])
    current_display_name = str(selected["nombre"])

    conflicting = [
        row
        for row in rows
        if int(row["station_id"]) != station_id
        and _normalize_existing_station_name(row["nombre"]) == new_norm
    ]
    if conflicting:
        names = ", ".join(f"{int(row['station_id'])}:{row['nombre']}" for row in conflicting)
        raise RenameError(f"New station name already exists in estaciones.db: {new_display_name!r}. Conflicts: {names}")

    return StationSelection(
        station_id=station_id,
        current_name=current_display_name,
        new_name=new_display_name,
    )


def build_preview(paths: RenamePaths, *, current_name: str, new_name: str) -> RenamePreview:
    stations_conn = _open_connection(paths.stations_db)
    assert stations_conn is not None
    try:
        _require_tables(stations_conn, paths.stations_db, ("Estaciones",))
        station = _resolve_station(stations_conn, current_name=current_name, new_name=new_name)
    finally:
        stations_conn.close()

    report_old_key = _normalize_report_station(station.current_name)
    report_new_key = _normalize_report_station(station.new_name)

    mensajes_conn = _open_connection(paths.mensajes_db)
    assert mensajes_conn is not None
    try:
        _require_tables(
            mensajes_conn,
            paths.mensajes_db,
            ("normalized_events", "pruebas", "alertas", "resolved_report_slots"),
        )
        normalized_events_count = _count_rows(
            mensajes_conn,
            "SELECT COUNT(*) FROM normalized_events WHERE station_id = ?",
            (station.station_id,),
        )
        pruebas_count = _count_rows(
            mensajes_conn,
            "SELECT COUNT(*) FROM pruebas WHERE station_id = ?",
            (station.station_id,),
        )
        alertas_count = _count_rows(
            mensajes_conn,
            "SELECT COUNT(*) FROM alertas WHERE station_id = ?",
            (station.station_id,),
        )
        resolved_slots_count = _count_rows(
            mensajes_conn,
            "SELECT COUNT(*) FROM resolved_report_slots WHERE station_id = ?",
            (station.station_id,),
        )
    finally:
        mensajes_conn.close()

    reportes_conn = _open_connection(paths.reportes_db)
    assert reportes_conn is not None
    try:
        _require_tables(reportes_conn, paths.reportes_db, ("reportes",))
        reportes_count = _count_rows(
            reportes_conn,
            """
            SELECT COUNT(*)
            FROM reportes
            WHERE UPPER(TRIM(estacion)) = ?
            """,
            (report_old_key,),
        )

        report_duplicate_old_dates: tuple[str, ...] = ()
        report_collision_dates: tuple[str, ...] = ()
        if report_old_key != report_new_key and reportes_count:
            report_duplicate_old_dates = _collect_dates(
                reportes_conn,
                """
                SELECT fecha
                FROM reportes
                WHERE UPPER(TRIM(estacion)) = ?
                GROUP BY fecha
                HAVING COUNT(*) > 1
                ORDER BY fecha ASC
                """,
                (report_old_key,),
            )
            report_collision_dates = _collect_dates(
                reportes_conn,
                """
                SELECT old_rows.fecha
                FROM reportes AS old_rows
                JOIN reportes AS new_rows
                  ON new_rows.fecha = old_rows.fecha
                WHERE UPPER(TRIM(old_rows.estacion)) = ?
                  AND UPPER(TRIM(new_rows.estacion)) = ?
                GROUP BY old_rows.fecha
                ORDER BY old_rows.fecha ASC
                """,
                (report_old_key, report_new_key),
            )
    finally:
        reportes_conn.close()

    sync_available = paths.sync_db.resolve().exists()
    sync_has_prune_candidates = False
    sync_old_count = 0
    sync_new_count = 0
    sync_conn = _open_connection(paths.sync_db, must_exist=False)
    if sync_conn is not None:
        try:
            sync_has_prune_candidates = _table_exists(sync_conn, "prune_candidates")
            if sync_has_prune_candidates:
                sync_old_count = _count_rows(
                    sync_conn,
                    "SELECT COUNT(*) FROM prune_candidates WHERE station = ?",
                    (report_old_key,),
                )
                sync_new_count = _count_rows(
                    sync_conn,
                    "SELECT COUNT(*) FROM prune_candidates WHERE station = ?",
                    (report_new_key,),
                )
        finally:
            sync_conn.close()

    return RenamePreview(
        station=station,
        paths=paths,
        report_old_key=report_old_key,
        report_new_key=report_new_key,
        normalized_events_count=normalized_events_count,
        pruebas_count=pruebas_count,
        alertas_count=alertas_count,
        resolved_slots_count=resolved_slots_count,
        reportes_count=reportes_count,
        sync_available=sync_available,
        sync_has_prune_candidates=sync_has_prune_candidates,
        sync_old_count=sync_old_count,
        sync_new_count=sync_new_count,
        report_collision_dates=report_collision_dates,
        report_duplicate_old_dates=report_duplicate_old_dates,
    )


def _print_preview(preview: RenamePreview) -> None:
    print("Rename Preview")
    print(f"  station_id: {preview.station.station_id}")
    print(f"  current name: {preview.station.current_name}")
    print(f"  new name: {preview.station.new_name}")
    if preview.report_key_changes:
        print(f"  reportes key: {preview.report_old_key} -> {preview.report_new_key}")
    else:
        print(f"  reportes key unchanged: {preview.report_old_key}")
    print("Affected rows")
    print("  estaciones.db / Estaciones: 1")
    print(f"  mensajes.db / normalized_events: {preview.normalized_events_count}")
    print(f"  mensajes.db / pruebas: {preview.pruebas_count}")
    print(f"  mensajes.db / alertas: {preview.alertas_count}")
    print(f"  mensajes.db / resolved_report_slots: {preview.resolved_slots_count}")
    print(f"  reportes.db / reportes: {preview.reportes_count}")
    if not preview.sync_available:
        print("  sync_tracking.db / prune_candidates: not found (skip)")
    elif not preview.sync_has_prune_candidates:
        print("  sync_tracking.db / prune_candidates: table missing (skip)")
    else:
        print(
            "  sync_tracking.db / prune_candidates: "
            f"old={preview.sync_old_count}, new={preview.sync_new_count}"
        )
    if preview.report_duplicate_old_dates:
        print(
            "  reportes duplicate dates for current station key: "
            + ", ".join(preview.report_duplicate_old_dates)
        )
    if preview.report_collision_dates:
        print(
            "  reportes collision dates for new station key (will merge): "
            + ", ".join(preview.report_collision_dates)
        )


def _ensure_no_conflicts(preview: RenamePreview) -> None:
    problems: list[str] = []
    if preview.report_duplicate_old_dates:
        problems.append(
            "reportes.db already has duplicate normalized rows for the current station on: "
            + ", ".join(preview.report_duplicate_old_dates)
        )
    if problems:
        raise RenameError("Cannot apply rename because " + " | ".join(problems))


def _prompt_for_value(label: str) -> str:
    while True:
        try:
            value = input(f"{label}: ")
        except EOFError as exc:
            raise RenameError(f"{label} is required.") from exc
        if value.strip():
            return value
        print(f"{label} cannot be blank.")


def _confirm(prompt: str) -> bool:
    try:
        response = input(f"{prompt} [y/N]: ")
    except EOFError as exc:
        raise RenameError("Confirmation is required to continue.") from exc
    return response.strip().lower() in {"y", "yes"}


def _backup_database(db_path: Path, timestamp: str) -> Path:
    resolved = db_path.resolve()
    backup_path = resolved.with_name(f"{resolved.stem}.backup-{timestamp}{resolved.suffix}")
    try:
        with sqlite3.connect(str(resolved), timeout=SQLITE_BUSY_TIMEOUT_MS / 1000) as source_conn:
            with sqlite3.connect(str(backup_path)) as backup_conn:
                source_conn.backup(backup_conn)
    except sqlite3.Error as exc:
        raise _sqlite_error(resolved, exc) from exc
    return backup_path


def _safe_rollback(conn: sqlite3.Connection | None) -> None:
    if conn is None:
        return
    with suppress(sqlite3.Error):
        conn.rollback()


def _backup_targets(preview: RenamePreview) -> dict[str, Path]:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    backups = {
        "estaciones": _backup_database(preview.paths.stations_db, timestamp),
        "mensajes": _backup_database(preview.paths.mensajes_db, timestamp),
        "reportes": _backup_database(preview.paths.reportes_db, timestamp),
    }
    if preview.sync_available:
        backups["sync_tracking"] = _backup_database(preview.paths.sync_db, timestamp)
    return backups


def _apply_monitor_updates(conn: sqlite3.Connection, preview: RenamePreview) -> None:
    conn.execute(
        "UPDATE normalized_events SET station_name = ? WHERE station_id = ?",
        (preview.station.new_name, preview.station.station_id),
    )
    conn.execute(
        "UPDATE pruebas SET station_name = ? WHERE station_id = ?",
        (preview.station.new_name, preview.station.station_id),
    )
    conn.execute(
        "UPDATE alertas SET station_name = ? WHERE station_id = ?",
        (preview.station.new_name, preview.station.station_id),
    )
    conn.execute(
        "UPDATE resolved_report_slots SET station_name = ? WHERE station_id = ?",
        (preview.station.new_name, preview.station.station_id),
    )


def _apply_reportes_update(conn: sqlite3.Connection, preview: RenamePreview) -> None:
    if not preview.report_key_changes:
        return

    for collision_date in preview.report_collision_dates:
        old_row = conn.execute(
            """
            SELECT id, consecutivos_perdidos, pruebas, prueba_time, disponibilidad, reportes
            FROM reportes
            WHERE fecha = ?
              AND UPPER(TRIM(estacion)) = ?
            """,
            (collision_date, preview.report_old_key),
        ).fetchone()
        new_row = conn.execute(
            """
            SELECT id, consecutivos_perdidos, pruebas, prueba_time, disponibilidad, reportes
            FROM reportes
            WHERE fecha = ?
              AND UPPER(TRIM(estacion)) = ?
            """,
            (collision_date, preview.report_new_key),
        ).fetchone()
        if old_row is None or new_row is None:
            continue
        if _reportes_quality(old_row) > _reportes_quality(new_row):
            conn.execute(
                """
                UPDATE reportes
                SET
                    consecutivos_perdidos = ?,
                    pruebas = ?,
                    prueba_time = ?,
                    disponibilidad = ?,
                    reportes = ?
                WHERE id = ?
                """,
                (
                    old_row["consecutivos_perdidos"],
                    old_row["pruebas"],
                    old_row["prueba_time"],
                    old_row["disponibilidad"],
                    old_row["reportes"],
                    new_row["id"],
                ),
            )
        conn.execute("DELETE FROM reportes WHERE id = ?", (old_row["id"],))

    conn.execute(
        """
        UPDATE reportes
        SET estacion = ?
        WHERE UPPER(TRIM(estacion)) = ?
        """,
        (preview.report_new_key, preview.report_old_key),
    )


def _apply_sync_update(conn: sqlite3.Connection, preview: RenamePreview) -> None:
    if not preview.report_key_changes or not preview.sync_has_prune_candidates:
        return

    old_row = conn.execute(
        """
        SELECT station, first_missing_at, last_missing_at, missing_scan_count
        FROM prune_candidates
        WHERE station = ?
        """,
        (preview.report_old_key,),
    ).fetchone()
    if old_row is None:
        return

    new_row = conn.execute(
        """
        SELECT station, first_missing_at, last_missing_at, missing_scan_count
        FROM prune_candidates
        WHERE station = ?
        """,
        (preview.report_new_key,),
    ).fetchone()
    if new_row is None:
        conn.execute(
            "UPDATE prune_candidates SET station = ? WHERE station = ?",
            (preview.report_new_key, preview.report_old_key),
        )
        return

    merged_first = min(str(old_row["first_missing_at"]), str(new_row["first_missing_at"]))
    merged_last = max(str(old_row["last_missing_at"]), str(new_row["last_missing_at"]))
    merged_count = int(old_row["missing_scan_count"]) + int(new_row["missing_scan_count"])
    conn.execute(
        """
        UPDATE prune_candidates
        SET first_missing_at = ?, last_missing_at = ?, missing_scan_count = ?
        WHERE station = ?
        """,
        (merged_first, merged_last, merged_count, preview.report_new_key),
    )
    conn.execute(
        "DELETE FROM prune_candidates WHERE station = ?",
        (preview.report_old_key,),
    )


def apply_rename(preview: RenamePreview) -> dict[str, Path]:
    backups = _backup_targets(preview)

    stations_conn: sqlite3.Connection | None = None
    mensajes_conn: sqlite3.Connection | None = None
    reportes_conn: sqlite3.Connection | None = None
    sync_conn: sqlite3.Connection | None = None
    active_path = preview.paths.stations_db
    try:
        active_path = preview.paths.stations_db
        stations_conn = _open_connection(preview.paths.stations_db)
        active_path = preview.paths.mensajes_db
        mensajes_conn = _open_connection(preview.paths.mensajes_db)
        active_path = preview.paths.reportes_db
        reportes_conn = _open_connection(preview.paths.reportes_db)
        active_path = preview.paths.sync_db
        sync_conn = _open_connection(preview.paths.sync_db, must_exist=False) if preview.sync_available else None

        active_path = preview.paths.stations_db
        assert stations_conn is not None
        stations_conn.execute("BEGIN IMMEDIATE")
        active_path = preview.paths.mensajes_db
        assert mensajes_conn is not None
        mensajes_conn.execute("BEGIN IMMEDIATE")
        active_path = preview.paths.reportes_db
        assert reportes_conn is not None
        reportes_conn.execute("BEGIN IMMEDIATE")
        if sync_conn is not None and preview.sync_has_prune_candidates:
            active_path = preview.paths.sync_db
            sync_conn.execute("BEGIN IMMEDIATE")

        active_path = preview.paths.stations_db
        stations_conn.execute(
            "UPDATE Estaciones SET nombre = ? WHERE station_id = ?",
            (preview.station.new_name, preview.station.station_id),
        )
        active_path = preview.paths.mensajes_db
        _apply_monitor_updates(mensajes_conn, preview)
        active_path = preview.paths.reportes_db
        _apply_reportes_update(reportes_conn, preview)
        if sync_conn is not None:
            active_path = preview.paths.sync_db
            _apply_sync_update(sync_conn, preview)

        active_path = preview.paths.sync_db
        if sync_conn is not None and preview.sync_has_prune_candidates:
            sync_conn.commit()
        active_path = preview.paths.reportes_db
        reportes_conn.commit()
        active_path = preview.paths.mensajes_db
        mensajes_conn.commit()
        active_path = preview.paths.stations_db
        stations_conn.commit()
        return backups
    except sqlite3.Error as exc:
        _safe_rollback(sync_conn)
        _safe_rollback(reportes_conn)
        _safe_rollback(mensajes_conn)
        _safe_rollback(stations_conn)
        raise _sqlite_error(active_path, exc) from exc
    finally:
        for conn in (sync_conn, reportes_conn, mensajes_conn, stations_conn):
            if conn is not None:
                conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Rename a station across Monitor and Reportes databases.")
    parser.add_argument("--current-name", help="Current station name in estaciones.db.")
    parser.add_argument("--new-name", help="Replacement station name.")
    parser.add_argument("--yes", action="store_true", help="Apply the rename without interactive confirmation.")
    parser.add_argument("--dry-run", action="store_true", help="Show the preview and validation results without writing.")
    parser.add_argument("--stations-db", default=str(STATIONS_DB), help="Path to estaciones.db")
    parser.add_argument("--mensajes-db", default=str(PARSED_DB), help="Path to mensajes.db")
    parser.add_argument("--reportes-db", default=str(DEFAULT_REPORTES_DB), help="Path to reportes.db")
    parser.add_argument("--sync-db", default=str(DEFAULT_SYNC_DB), help="Path to optional sync_tracking.db")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        current_name = args.current_name or _prompt_for_value("Current station name")
        new_name = args.new_name or _prompt_for_value("New station name")
        paths = RenamePaths(
            stations_db=Path(args.stations_db),
            mensajes_db=Path(args.mensajes_db),
            reportes_db=Path(args.reportes_db),
            sync_db=Path(args.sync_db),
        )
        preview = build_preview(paths, current_name=current_name, new_name=new_name)
        _print_preview(preview)
        _ensure_no_conflicts(preview)

        if args.dry_run:
            print("Dry run complete. No changes were written.")
            return 0

        if not args.yes and not _confirm("Proceed with the rename"):
            print("Rename cancelled.")
            return 0

        backups = apply_rename(preview)
        print("Rename completed successfully.")
        for label, backup_path in backups.items():
            print(f"  backup {label}: {backup_path}")
        return 0
    except RenameError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
