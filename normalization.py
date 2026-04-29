"""
normalization.py - Source-specific normalization into unified schema.
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

from config import LOCAL_TIMEZONE, CONFIDENCE_RESOLVE_THRESHOLD
import logging

from models import MessageType, NormalizedEvent, SerialParsed, Source, TelegramParsed
from parser_engine import extract_channel_number
from schedule_engine import get_window_range
from station_manager import StationManager
from station_resolver import StationResolver

logger = logging.getLogger(__name__)


def _format_local(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _get_local_tz():
    try:
        return ZoneInfo(LOCAL_TIMEZONE)
    except Exception as exc:
        logger.warning(
            "ZoneInfo lookup failed for %s (%s). Falling back to system local tz.",
            LOCAL_TIMEZONE,
            exc,
        )
        sys_tz = datetime.now().astimezone().tzinfo
        if sys_tz is None:
            logger.warning("System local tzinfo unavailable; falling back to UTC.")
            return timezone.utc
        return sys_tz


def _to_utc_iso(local_dt: datetime) -> str:
    return local_dt.astimezone(timezone.utc).isoformat()


def _local_report_date(local_dt: datetime) -> str:
    return local_dt.date().isoformat()


def _compute_slot(local_dt: datetime) -> tuple[str | None, bool]:
    window = get_window_range(local_dt)
    if not window:
        return None, False
    start, _ = window
    return start.strftime("%H:%M"), True


def _mask_phone(phone: str) -> str:
    if len(phone) <= 4:
        return phone
    return f"{phone[:2]}***{phone[-2:]}"


def validate_serial_timestamp_fields(parsed: SerialParsed) -> None:
    if not 1 <= parsed.julian_day <= 366:
        raise ValueError(f"serial_invalid_julian_day:{parsed.julian_day}")
    if not 0 <= parsed.hour <= 23:
        raise ValueError(f"serial_invalid_hour:{parsed.hour}")
    if not 0 <= parsed.minute <= 59:
        raise ValueError(f"serial_invalid_minute:{parsed.minute}")


def _classify_telegram(
    parsed: TelegramParsed,
    station_name: str,
    station_manager: StationManager,
) -> tuple[MessageType, str | None, str]:
    if parsed.is_mensaje:
        if station_manager.get_tx_sarmex(station_name) == 2:
            return MessageType.SINGLE, None, parsed.content
        channel = extract_channel_number(parsed.mensaje_channel_raw)
        return MessageType.RWT, channel, parsed.mensaje_text or parsed.content

    open_text, close_text = station_manager.get_open_close(station_name)
    if open_text:
        for token in open_text.split(","):
            if token.strip() and token.strip() in parsed.content:
                return MessageType.OPEN, None, parsed.content
    if close_text:
        for token in close_text.split(","):
            if token.strip() and token.strip() in parsed.content:
                return MessageType.CLOSE, None, parsed.content
    return MessageType.SINGLE, None, parsed.content


def normalize_telegram(
    parsed: TelegramParsed,
    raw_event_id: int,
    station_manager: StationManager,
    resolver: StationResolver,
) -> NormalizedEvent:
    resolution = resolver.resolve_from_phone(parsed.phone, parsed.mensaje_station_hint)
    station_name = resolution.station_name

    msg_type, channel, message_text = _classify_telegram(
        parsed, station_name, station_manager
    )

    local_tz = _get_local_tz()
    local_dt_naive = parsed.timestamp
    local_dt = parsed.timestamp.replace(tzinfo=local_tz)
    report_date = _local_report_date(local_dt_naive)
    report_slot, tone = _compute_slot(local_dt_naive)

    if msg_type == MessageType.RWT:
        event_class = "TEST"
    elif msg_type in (MessageType.OPEN, MessageType.CLOSE):
        event_class = "STATE"
    else:
        event_class = "INFO"

    base_confidence = 80
    confidence = max(10, base_confidence - resolution.confidence_penalty)
    if resolution.resolved_by in ("phone_ambiguous_default", "phone_ambiguous_unresolved"):
        logger.warning(
            "Station ambiguity for phone %s resolved_by=%s",
            _mask_phone(parsed.phone),
            resolution.resolved_by,
        )

    station_id = resolution.station_id
    if resolution.ambiguous and confidence < CONFIDENCE_RESOLVE_THRESHOLD:
        logger.warning(
            "Station confidence %d below threshold %d; storing station_id=NULL",
            confidence,
            CONFIDENCE_RESOLVE_THRESHOLD,
        )
        station_id = None

    is_valid_report = tone and station_id is not None

    return NormalizedEvent(
        raw_event_id=raw_event_id,
        source=Source.TELEGRAM,
        station_id=station_id,
        station_name=station_name,
        station_code=resolution.station_code,
        event_type=msg_type.value,
        event_class=event_class,
        report_date=report_date,
        report_slot=report_slot,
        event_time_local=_format_local(local_dt_naive),
        event_time_utc=_to_utc_iso(local_dt),
        tone=tone,
        priority=100,
        is_valid_report=is_valid_report,
        parser_version="telegram:v1.2",
        confidence_score=confidence,
        phone_number=parsed.phone,
        channel=channel,
        payload_json={
            "content": parsed.content,
            "mensaje_text": message_text,
            "station_hint": parsed.mensaje_station_hint,
            "is_mensaje": parsed.is_mensaje,
            "station_resolution_method": resolution.resolved_by,
            "station_resolution_confidence": confidence,
            "station_resolution_threshold": CONFIDENCE_RESOLVE_THRESHOLD,
        },
    )


def _julian_to_datetime(year: int, julian_day: int, hour: int, minute: int) -> datetime:
    base = datetime(year, 1, 1, tzinfo=timezone.utc)
    return base.replace(hour=0, minute=0, second=0, microsecond=0) + (
        timedelta(days=julian_day - 1, hours=hour, minutes=minute)
    )


def normalize_serial(
    parsed: SerialParsed,
    raw_event_id: int,
    resolver: StationResolver,
    received_at: datetime,
) -> NormalizedEvent:
    validate_serial_timestamp_fields(parsed)
    resolution = resolver.resolve_from_transmitter(parsed.transmitter_code)
    local_tz = _get_local_tz()

    inferred_year = received_at.year
    event_time_utc = _julian_to_datetime(
        inferred_year, parsed.julian_day, parsed.hour, parsed.minute
    )
    event_time_local = event_time_utc.astimezone(local_tz)
    event_time_local_naive = event_time_local.replace(tzinfo=None)

    report_date = _local_report_date(event_time_local_naive)
    report_slot = None
    tone = False
    if parsed.event_code == "RWT":
        report_slot, tone = _compute_slot(event_time_local_naive)

    if parsed.event_code == "RWT":
        event_class = "TEST"
    elif parsed.event_code == "EQW":
        event_class = "ALERT"
    else:
        event_class = "INFO"

    is_valid_report = tone and resolution.station_id is not None

    return NormalizedEvent(
        raw_event_id=raw_event_id,
        source=Source.SERIAL,
        station_id=resolution.station_id,
        station_name=resolution.station_name,
        station_code=resolution.station_code,
        event_type=parsed.event_code,
        event_class=event_class,
        report_date=report_date,
        report_slot=report_slot,
        event_time_local=_format_local(event_time_local_naive),
        event_time_utc=event_time_utc.isoformat(),
        tone=tone,
        priority=200,
        is_valid_report=is_valid_report,
        parser_version="serial:v1",
        confidence_score=60,
        transmitter_code=parsed.transmitter_code,
        channel=resolution.channel,
        payload_json={
            "originator": parsed.originator,
            "area_codes": parsed.area_codes,
            "duration_code": parsed.duration_code,
            "raw_header": parsed.raw_header,
            "repeat_count": parsed.repeat_count,
            "time_inference_method": "received_at_year",
        },
    )
