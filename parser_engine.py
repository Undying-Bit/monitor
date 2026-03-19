"""
parser_engine.py — Two-tier regex parser for Telegram messages.

Tier 1: Base extraction (phone, date, time, content) — common to all.
Tier 2: "MENSAJE" (Type B) extraction — station name and channel from body.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional, TYPE_CHECKING

from models import ParsedMessage, MessageType

if TYPE_CHECKING:
    from station_manager import StationManager

logger = logging.getLogger(__name__)

# ── Tier 1 regex ─────────────────────────────────────────────
# Matches: +PHONE DATE TIME CONTENT
TIER1_RE = re.compile(
    r"^(?P<phone>\+\d+)\s+"
    r"(?P<date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2}:\d{2})\s+"
    r"(?P<content>.*)$"
)

# ── Tier 2 regex (Type B / MENSAJE) ──────────────────────────
# Matches: MENSAJE **/07/21 HH:MM:SS <text> STATION_NAME <channel>
TIER2_RE = re.compile(
    r"MENSAJE\s+"
    r"[\*\d/]+\s+"      # internal date (may contain asterisks)
    r"[\d:]+\s+"        # internal time
    r"(?P<text>.*?)\s+"
    r"(?P<station>[A-ZÁÉÍÓÚÑ]+)\s+"
    r"(?P<channel>(?:canal\s+\d+|CH-\d+))$",
    re.IGNORECASE,
)

# ── Logging for parse failures ───────────────────────────────
_parse_error_logger: Optional[logging.Logger] = None


def _get_parse_error_logger() -> logging.Logger:
    """Lazy-init a dedicated logger that writes to parsing_errors.log."""
    global _parse_error_logger
    if _parse_error_logger is None:
        from config import PARSING_ERRORS_LOG
        _parse_error_logger = logging.getLogger("parsing_errors")
        _parse_error_logger.setLevel(logging.WARNING)
        if not _parse_error_logger.handlers:
            fh = logging.FileHandler(str(PARSING_ERRORS_LOG), encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
            _parse_error_logger.addHandler(fh)
    return _parse_error_logger


def parse(
    raw_text: str,
    station_manager: StationManager,
    telegram_id: int = 0,
) -> Optional[ParsedMessage]:
    """
    Full parsing pipeline.

    Returns a validated ParsedMessage or None if the text is unparseable.
    """
    if not raw_text or not isinstance(raw_text, str):
        return None

    # ── Tier 1: base extraction ──────────────────────────────
    m1 = TIER1_RE.match(raw_text.strip())
    if not m1:
        _get_parse_error_logger().warning("TIER1_FAIL | %s", raw_text)
        return None

    phone_raw = m1.group("phone")
    date_str = m1.group("date")
    time_str = m1.group("time")
    content = m1.group("content")

    # Normalize phone: strip leading + and country-code prefix
    phone = _normalize_phone(phone_raw)

    # Build internal timestamp
    try:
        timestamp = datetime.strptime(f"{date_str} {time_str}", "%d/%m/%Y %H:%M:%S")
    except ValueError:
        _get_parse_error_logger().warning("DATE_FAIL | %s", raw_text)
        return None

    # ── Station identification ───────────────────────────────
    stations = station_manager.lookup_by_phone(phone)

    if not stations:
        # Unknown phone — still store with a placeholder name
        station_name = f"Estacion {phone}"
    elif len(stations) == 1:
        station_name = stations[0]
    else:
        # Ambiguous: try Tier 2 to resolve
        station_name = _resolve_ambiguous(content, stations, station_manager)

    # ── Classification ───────────────────────────────────────
    message_type, channel, message_text = _classify(
        content, station_name, station_manager
    )

    return ParsedMessage(
        telegram_id=telegram_id,
        telefono=phone,
        estacion=station_name,
        red=station_manager.get_red(station_name),
        tipo_mensaje=message_type,
        canal=channel,
        texto=message_text or content,
        timestamp=timestamp,
        # tono will be set by ScheduleEngine later
    )


# ── Helpers ──────────────────────────────────────────────────


def _normalize_phone(phone_raw: str) -> str:
    """Strip '+' and country code, keep last PHONE_LENGTH digits."""
    from config import PHONE_LENGTH
    digits = phone_raw.lstrip("+")
    if digits.startswith("52"):
        digits = digits[2:]
    return digits[:PHONE_LENGTH]


def _resolve_ambiguous(
    content: str,
    candidates: list[str],
    station_manager: StationManager,
) -> str:
    """Use Tier 2 regex to pick the right station from ambiguous matches."""
    m2 = TIER2_RE.search(content)
    if m2:
        extracted_name = m2.group("station")
        for candidate in candidates:
            if extracted_name.upper() in candidate.upper():
                return candidate
    # Fallback: return first candidate
    return candidates[0]


def _classify(
    content: str,
    station_name: str,
    station_manager: StationManager,
) -> tuple[MessageType, Optional[str], Optional[str]]:
    """
    Determine message type, channel, and cleaned text.

    Returns (MessageType, channel_or_None, message_text_or_None).
    """
    # Check for Type B (MENSAJE)
    m2 = TIER2_RE.search(content)
    if m2:
        if station_manager.get_tx_sarmex(station_name) == 2:
            return MessageType.SINGLE, None, content
        channel = _extract_channel_number(m2.group("channel"))
        return MessageType.RWT, channel, m2.group("text")

    # Check open / close from station config
    open_text, close_text = station_manager.get_open_close(station_name)

    if open_text:
        # open_text may be comma-separated (e.g. "Puebla ALT,Puebla SUP")
        for token in open_text.split(","):
            if token.strip() and token.strip() in content:
                return MessageType.OPEN, None, content

    if close_text:
        for token in close_text.split(","):
            if token.strip() and token.strip() in content:
                return MessageType.CLOSE, None, content

    return MessageType.SINGLE, None, content


def _extract_channel_number(channel_raw: str) -> Optional[str]:
    """Return only the numeric part from channel tokens like 'canal 2' or 'CH-3'."""
    match = re.search(r"\d+", channel_raw or "")
    if match:
        return match.group(0)
    return None
