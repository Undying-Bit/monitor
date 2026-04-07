"""
parser_engine.py - Telegram parsing (regex-based).

Tier 1: Base extraction (phone, date, time, content).
Tier 2: "MENSAJE" extraction (station hint + channel).
"""
from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional

from models import TelegramParsed

logger = logging.getLogger(__name__)

# ── Tier 1 regex ────────────────────────────────────────────────────────────
# Matches: +PHONE DATE TIME CONTENT
TIER1_RE = re.compile(
    r"^(?P<phone>\+\d+)\s+"
    r"(?P<date>\d{2}/\d{2}/\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2}:\d{2})\s+"
    r"(?P<content>.*)$"
)

# ── Tier 2 regex (Type B / MENSAJE) ─────────────────────────────────────────
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

# ── Logging for parse failures ──────────────────────────────────────────────
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


def parse_telegram(raw_text: str) -> Optional[TelegramParsed]:
    """
    Parse Telegram message content into a TelegramParsed object.

    Returns None if the text is unparseable.
    """
    if not raw_text or not isinstance(raw_text, str):
        return None

    # ── Tier 1: base extraction ────────────────────────────────────────────
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

    # ── Tier 2 hint extraction ─────────────────────────────────────────────
    m2 = TIER2_RE.search(content)
    is_mensaje = m2 is not None
    station_hint = m2.group("station") if m2 else None
    channel_raw = m2.group("channel") if m2 else None
    mensaje_text = m2.group("text") if m2 else None

    return TelegramParsed(
        phone=phone,
        timestamp=timestamp,
        content=content,
        is_mensaje=is_mensaje,
        mensaje_station_hint=station_hint,
        mensaje_channel_raw=channel_raw,
        mensaje_text=mensaje_text,
    )


def _normalize_phone(phone_raw: str) -> str:
    """Strip '+' and country code, keep last PHONE_LENGTH digits."""
    from config import PHONE_LENGTH
    digits = phone_raw.lstrip("+")
    if digits.startswith("52"):
        digits = digits[2:]
    return digits[:PHONE_LENGTH]


def _extract_channel_number(channel_raw: str) -> Optional[str]:
    """Return only the numeric part from channel tokens like 'canal 2' or 'CH-3'."""
    match = re.search(r"\d+", channel_raw or "")
    if match:
        return match.group(0)
    return None


def extract_channel_number(channel_raw: Optional[str]) -> Optional[str]:
    """Public helper for channel normalization."""
    if not channel_raw:
        return None
    return _extract_channel_number(channel_raw)
