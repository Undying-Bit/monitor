"""
models.py - Pydantic data models for the monitoring pipeline.
"""
from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Source(str, Enum):
    TELEGRAM = "telegram"
    SERIAL = "serial"


class MessageType(str, Enum):
    """Telegram message classification."""
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    RWT = "RWT"        # Type-B / MENSAJE that isn't open/close
    SINGLE = "SINGLE"  # Unmatched content


class RawEvent(BaseModel):
    """Ingress-level representation for any source."""
    source: Source
    source_event_id: Optional[str] = None
    raw_payload: str
    received_at: datetime
    transport_meta: Optional[dict] = None


class TelegramParsed(BaseModel):
    """Parsed Telegram payload (before station resolution)."""
    phone: str
    timestamp: datetime
    content: str
    is_mensaje: bool = False
    mensaje_station_hint: Optional[str] = None
    mensaje_channel_raw: Optional[str] = None
    mensaje_text: Optional[str] = None


class SerialParsed(BaseModel):
    """Parsed SAME/EAS header from serial payload."""
    originator: str
    event_code: str
    area_codes: list[str]
    duration_code: str
    julian_day: int
    hour: int
    minute: int
    transmitter_code: str
    raw_header: str
    repeat_count: int = 1


class NormalizedEvent(BaseModel):
    """Normalized event ready for persistence."""
    raw_event_id: int
    source: Source
    station_id: Optional[int] = None
    station_name: Optional[str] = None
    station_code: Optional[str] = None

    event_type: str
    event_class: str

    report_date: Optional[str] = None
    report_slot: Optional[str] = None

    event_time_local: str
    event_time_utc: Optional[str] = None

    tone: bool = False
    priority: int = 0
    is_valid_report: bool = False

    parser_version: str = "unknown"
    confidence_score: int = 0

    transmitter_code: Optional[str] = None
    phone_number: Optional[str] = None
    channel: Optional[str] = None
    payload_json: dict = Field(default_factory=dict)
