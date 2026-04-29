"""
station_resolver.py - Centralized station resolution logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import logging
import unicodedata

from station_manager import StationManager, Station


@dataclass(frozen=True)
class StationResolution:
    station_id: Optional[int]
    station_name: str
    station_code: Optional[str] = None
    channel: Optional[str] = None
    resolved_by: str = "unknown"
    confidence_penalty: int = 0
    ambiguous: bool = False


logger = logging.getLogger(__name__)


def _mask_phone(phone: str) -> str:
    if len(phone) <= 4:
        return phone
    return f"{phone[:2]}***{phone[-2:]}"


TRANSMITTER_MAP: dict[str, dict[str, str]] = {
    "XCMX/011": {"name": "Teuhtli", "channel": "3"},
    "XCMX/004": {"name": "Cuajimalpa", "channel": "5"},
    "XCMX/005": {"name": "Zacatenco", "channel": "7"},
    "XMEX/037": {"name": "La Palma", "channel": "6"},
    "XCMX/003": {"name": "CENAPRED", "channel": "1"},
    "XMEX/048": {"name": "Jocotitlan", "channel": "2"},
}

LEGACY_STATION_ALIASES: dict[str, str] = {
    "TEUTLI": "TEUHTLI",
}


def _canonical_station_token(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(char for char in normalized if not unicodedata.combining(char))
    compact = "".join(char for char in ascii_only.upper() if char.isalnum())
    return LEGACY_STATION_ALIASES.get(compact, compact)


def _match_station_by_hint(stations: list[Station], station_hint: str) -> Station | None:
    hint_key = _canonical_station_token(station_hint)
    if not hint_key:
        return None
    for station in stations:
        station_key = _canonical_station_token(station.nombre)
        if hint_key and (hint_key in station_key or station_key in hint_key):
            return station
    return None


def _match_station_by_name(station_manager: StationManager, station_name: str) -> Station | None:
    direct = station_manager.get_station(station_name)
    if direct is not None:
        return direct
    direct = station_manager.get_station_case_insensitive(station_name)
    if direct is not None:
        return direct

    target_key = _canonical_station_token(station_name)
    if not target_key:
        return None
    for station in station_manager.get_all_stations():
        if _canonical_station_token(station.nombre) == target_key:
            return station
    return None


class StationResolver:
    """Centralized station resolution for all sources."""

    def __init__(self, station_manager: StationManager) -> None:
        self._sm = station_manager

    def resolve_from_phone(
        self,
        phone: str,
        station_hint: Optional[str] = None,
    ) -> StationResolution:
        stations = self._sm.lookup_stations_by_phone(phone)
        if not stations:
            return StationResolution(
                station_id=None,
                station_name=f"Estacion {phone}",
                resolved_by="unknown_phone",
            )
        if len(stations) == 1:
            st = stations[0]
            return StationResolution(
                station_id=st.station_id,
                station_name=st.nombre,
                resolved_by="phone_unique",
            )
        if station_hint:
            matched = _match_station_by_hint(stations, station_hint)
            if matched is not None:
                return StationResolution(
                    station_id=matched.station_id,
                    station_name=matched.nombre,
                    resolved_by="phone_hint",
                )
        logger.warning(
            "Ambiguous phone %s has %d candidates; storing station_id=NULL",
            _mask_phone(phone),
            len(stations),
        )
        return StationResolution(
            station_id=None,
            station_name=f"Estacion {phone}",
            resolved_by="phone_ambiguous_unresolved",
            confidence_penalty=60,
            ambiguous=True,
        )

    def resolve_from_transmitter(self, transmitter_code: str) -> StationResolution:
        info = TRANSMITTER_MAP.get(transmitter_code.upper())
        if not info:
            return StationResolution(
                station_id=None,
                station_name=f"Transmitter {transmitter_code}",
                station_code=transmitter_code,
                resolved_by="unknown_transmitter",
            )
        station_name = info.get("name", "")
        channel = info.get("channel")
        st = _match_station_by_name(self._sm, station_name) if station_name else None
        return StationResolution(
            station_id=st.station_id if st else None,
            station_name=st.nombre if st else station_name,
            station_code=transmitter_code,
            channel=channel,
            resolved_by="transmitter_map",
        )
