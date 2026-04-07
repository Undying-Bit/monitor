"""
station_manager.py — Caching layer for estaciones.db.

Loads all stations on init, provides fast lookups by phone number,
ambiguity resolution, and open/close text retrieval.
"""
from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from config import STATIONS_DB, PHONE_LENGTH

logger = logging.getLogger(__name__)


@dataclass
class Station:
    """In-memory representation of a station row."""
    station_id: int
    nombre: str
    telefono: str  # normalized (no country code)
    open_text: str
    close_text: str
    tx_sarmex: int = 0
    red: str = ""


class StationManager:
    """
    Reads estaciones.db once at construction and caches everything.

    Provides:
      - lookup_by_phone(phone) → list of station names (usually 1)
      - get_open_close(station_name) → (open_text, close_text)
      - resolve_ambiguity(phone, name_hint) → station_name
    """

    def __init__(self, db_path: Path | None = None) -> None:
        self._db_path = db_path or STATIONS_DB
        self._by_phone: dict[str, list[Station]] = {}
        self._by_name: dict[str, Station] = {}
        self._load()

    # ── Public API ───────────────────────────────────────────

    def lookup_by_phone(self, phone: str) -> list[str]:
        """Return station names that share this phone number."""
        stations = self._by_phone.get(phone, [])
        return [s.nombre for s in stations]

    def lookup_stations_by_phone(self, phone: str) -> list[Station]:
        """Return Station objects that share this phone number."""
        return list(self._by_phone.get(phone, []))

    def get_station(self, station_name: str) -> Optional[Station]:
        """Return the Station object by name."""
        return self._by_name.get(station_name)

    def get_station_case_insensitive(self, station_name: str) -> Optional[Station]:
        """Return the Station object by name (case-insensitive)."""
        for name, st in self._by_name.items():
            if name.upper() == station_name.upper():
                return st
        return None

    def get_open_close(self, station_name: str) -> tuple[str, str]:
        """Return (open_text, close_text) for a station."""
        st = self._by_name.get(station_name)
        if st:
            return st.open_text or "", st.close_text or ""
        return "", ""

    def get_red(self, station_name: str) -> str:
        """Return the 'red' assigned to a station."""
        st = self._by_name.get(station_name)
        if st:
            return st.red or ""
        return ""

    def get_tx_sarmex(self, station_name: str) -> int:
        """Return the 'tx sarmex' value for a station (often used as 'type')."""
        st = self._by_name.get(station_name)
        if st:
            return st.tx_sarmex or 0
        return 0

    def resolve_ambiguity(self, phone: str, name_hint: str) -> str:
        """Given an ambiguous phone, use name_hint to pick the right station."""
        candidates = self._by_phone.get(phone, [])
        for st in candidates:
            if name_hint.upper() in st.nombre.upper():
                return st.nombre
        return candidates[0].nombre if candidates else f"Estacion {phone}"

    def get_all_phones(self) -> dict[str, str]:
        """Return {phone: station_name} dict (for single-station phones)."""
        result: dict[str, str] = {}
        for phone, stations in self._by_phone.items():
            if len(stations) == 1:
                result[phone] = stations[0].nombre
            else:
                for st in stations:
                    result[phone] = st.nombre  # last wins; caller uses lookup
        return result

    # ── Internal ─────────────────────────────────────────────

    def _load(self) -> None:
        """Read estaciones.db and populate caches."""
        if not self._db_path.exists():
            raise FileNotFoundError(f"estaciones.db not found at {self._db_path}")

        try:
            with sqlite3.connect(str(self._db_path)) as conn:
                self._validate_station_schema(conn)
                cursor = conn.cursor()
                cursor.execute(
                    'SELECT station_id, nombre, telefono, "open", "close", "tx sarmex", red '
                    "FROM Estaciones"
                )

                seen_station_ids: set[int] = set()
                for row in cursor.fetchall():
                    station_id_raw, nombre, telefono_raw, open_t, close_t, tx, red = row
                    if station_id_raw is None:
                        raise RuntimeError(
                            "station_id cannot be NULL in Estaciones"
                        )

                    station_id = int(station_id_raw)
                    if station_id in seen_station_ids:
                        raise RuntimeError(
                            f"Duplicate station_id detected: {station_id}"
                        )
                    seen_station_ids.add(station_id)

                    phone = self._normalize(str(telefono_raw))
                    st = Station(
                        station_id=station_id,
                        nombre=nombre or "",
                        telefono=phone,
                        open_text=open_t or "",
                        close_text=close_t or "",
                        tx_sarmex=tx or 0,
                        red=red or "",
                    )

                    self._by_phone.setdefault(phone, []).append(st)
                    self._by_name[st.nombre] = st

            logger.info(
                "StationManager loaded %d stations (%d unique phones)",
                len(self._by_name),
                len(self._by_phone),
            )

        except Exception as exc:
            logger.error("Failed to load estaciones.db: %s", exc)
            raise

    @staticmethod
    def _validate_station_schema(conn: sqlite3.Connection) -> None:
        """Fail fast when the station reference data is not ready for runtime use."""
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(Estaciones)")
        cols = [row[1] for row in cursor.fetchall()]
        if not cols:
            raise RuntimeError("Estaciones table is missing from estaciones.db")
        if "station_id" not in cols:
            raise RuntimeError(
                "Estaciones is missing the required station_id column"
            )

    @staticmethod
    def _normalize(phone_raw: str) -> str:
        """Strip country code prefix, keep PHONE_LENGTH digits."""
        digits = phone_raw.lstrip("+")
        if digits.startswith("52"):
            digits = digits[2:]
        return digits[:PHONE_LENGTH]
