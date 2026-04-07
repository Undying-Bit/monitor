"""
station_resolver.py - Centralized station resolution logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import logging

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


TRANSMITTER_MAP: dict[str, dict[str, str]] = {
    "XCMX/011": {"name": "TEUTLI", "channel": "3"},
    "XCMX/004": {"name": "Cuajimalpa", "channel": "5"},
    "XCMX/005": {"name": "Zacatenco", "channel": "7"},
    "XMEX/037": {"name": "La Palma", "channel": "6"},
    "XCMX/003": {"name": "CENAPRED", "channel": "1"},
    "XMEX/048": {"name": "Jocotitlan", "channel": "2"},
}


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
            for st in stations:
                if station_hint.upper() in st.nombre.upper():
                    return StationResolution(
                        station_id=st.station_id,
                        station_name=st.nombre,
                        resolved_by="phone_hint",
                    )
        # Ambiguous without a hint: choose best available match with lowered confidence.
        if len(stations) <= 2:
            st = stations[0]
            logger.warning(
                "Ambiguous phone %s resolved to %s with lowered confidence",
                phone,
                st.nombre,
            )
            return StationResolution(
                station_id=st.station_id,
                station_name=st.nombre,
                resolved_by="phone_ambiguous_default",
                confidence_penalty=10,
                ambiguous=True,
            )
        # Ambiguity too high: keep station_id NULL
        logger.warning(
            "Ambiguous phone %s has %d candidates; storing station_id=NULL",
            phone,
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
        st = self._sm.get_station(station_name) if station_name else None
        if not st and station_name:
            st = self._sm.get_station_case_insensitive(station_name)
        return StationResolution(
            station_id=st.station_id if st else None,
            station_name=st.nombre if st else station_name,
            station_code=transmitter_code,
            channel=channel,
            resolved_by="transmitter_map",
        )
