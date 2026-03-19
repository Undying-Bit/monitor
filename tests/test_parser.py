"""
tests/test_parser.py — Unit tests for the ParserEngine.

Covers:
  - Tier 1 base extraction (phone, date, time, content)
  - Tier 2 MENSAJE extraction (station name, channel)
  - Malformed / unparseable input → returns None
  - OPEN / CLOSE classification via station config
  - SINGLE fallback when content doesn't match open/close
"""
import pytest
from unittest.mock import MagicMock

# Ensure project root is on path
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from parser_engine import parse, TIER1_RE, TIER2_RE
from models import MessageType


# ── Helpers ──────────────────────────────────────────────────

def _make_station_manager(
    phone_map=None,
    open_close=None,
):
    """Create a mock StationManager with controllable responses."""
    sm = MagicMock()

    _phone_map = phone_map or {}
    _open_close = open_close or {}

    sm.lookup_by_phone.side_effect = lambda p: _phone_map.get(p, [])
    sm.get_open_close.side_effect = lambda n: _open_close.get(n, ("", ""))
    sm.get_red.side_effect = lambda n: ""
    sm.get_tx_sarmex.return_value = 0

    return sm


# ── Tier 1 regex tests ──────────────────────────────────────

class TestTier1Regex:
    def test_basic_match(self):
        text = "+5561048913 17/03/2026 10:45:00 COLIMA SUP"
        m = TIER1_RE.match(text)
        assert m is not None
        assert m.group("phone") == "+5561048913"
        assert m.group("date") == "17/03/2026"
        assert m.group("time") == "10:45:00"
        assert m.group("content") == "COLIMA SUP"

    def test_single_digit_hour_match(self):
        text = "+525561048913 18/03/2026 8:45:19 Restablecimiento"
        m = TIER1_RE.match(text)
        assert m is not None
        assert m.group("time") == "8:45:19"
        assert m.group("content") == "Restablecimiento"

    def test_long_content(self):
        text = "+529876543210 01/01/2026 23:45:30 MENSAJE **/07/21 12:00:00 some text STATION canal 1"
        m = TIER1_RE.match(text)
        assert m is not None
        assert m.group("content").startswith("MENSAJE")

    def test_no_match_missing_time(self):
        text = "+5561048913 17/03/2026 COLIMA SUP"
        m = TIER1_RE.match(text)
        assert m is None

    def test_no_match_empty(self):
        m = TIER1_RE.match("")
        assert m is None


# ── Tier 2 regex tests ──────────────────────────────────────

class TestTier2Regex:
    def test_mensaje_with_canal(self):
        content = "MENSAJE **/07/21 12:00:00 alerta sísmica PUEBLA canal 1"
        m = TIER2_RE.search(content)
        assert m is not None
        assert m.group("station") == "PUEBLA"
        assert m.group("channel") == "canal 1"

    def test_mensaje_with_ch_dash(self):
        content = "MENSAJE 01/07/21 08:45:00 test message TOLUCA CH-3"
        m = TIER2_RE.search(content)
        assert m is not None
        assert m.group("station") == "TOLUCA"
        assert m.group("channel") == "CH-3"

    def test_mensaje_malformed_date_asterisks(self):
        content = "MENSAJE **/07/21 08:45:00 prueba COLIMA canal 2"
        m = TIER2_RE.search(content)
        assert m is not None
        assert m.group("text") == "prueba"
        assert m.group("station") == "COLIMA"

    def test_no_match_regular_content(self):
        content = "COLIMA SUP"
        m = TIER2_RE.search(content)
        assert m is None


# ── Full parse() pipeline tests ──────────────────────────────

class TestParsePipeline:
    def test_open_message(self):
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
            open_close={"PC Colima": ("COLIMA SUP", "Restablecimiento")},
        )
        result = parse(
            "+5561048913 17/03/2026 14:45:00 COLIMA SUP",
            sm,
            telegram_id=1001,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.OPEN
        assert result.estacion == "PC Colima"
        assert result.telefono == "5561048913"
        assert result.timestamp.hour == 14
        assert result.timestamp.minute == 45

    def test_parse_single_digit_hour(self):
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
            open_close={"PC Colima": ("COLIMA SUP", "Restablecimiento")},
        )
        result = parse(
            "+5561048913 18/03/2026 8:45:19 Restablecimiento",
            sm,
        )
        assert result is not None
        assert result.timestamp.hour == 8
        assert result.timestamp.minute == 45
        assert result.tipo_mensaje == MessageType.CLOSE

    def test_close_message(self):
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
            open_close={"PC Colima": ("COLIMA SUP", "Restablecimiento")},
        )
        result = parse(
            "+5561048913 17/03/2026 14:47:00 Restablecimiento",
            sm,
            telegram_id=1002,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.CLOSE

    def test_mensaje_type_b_rwt(self):
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
            open_close={"PC Colima": ("COLIMA SUP", "Restablecimiento")},
        )
        result = parse(
            "+5561048913 17/03/2026 12:00:00 MENSAJE **/07/21 12:00:00 prueba COLIMA canal 2",
            sm,
            telegram_id=1003,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.RWT
        assert result.canal == "2"

    def test_mensaje_type_b_ch_dash_stores_channel_number(self):
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
            open_close={"PC Colima": ("COLIMA SUP", "Restablecimiento")},
        )
        result = parse(
            "+5561048913 17/03/2026 12:00:00 MENSAJE 01/07/21 08:45:00 prueba COLIMA CH-3",
            sm,
            telegram_id=1008,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.RWT
        assert result.canal == "3"

    def test_type_2_rwt_as_single(self):
        """Stations with tx_sarmex=2 (Type 2) should classify RWT-patterned msgs as SINGLE."""
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
        )
        sm.get_tx_sarmex.return_value = 2
        
        result = parse(
            "+5561048913 17/03/2026 12:00:00 MENSAJE **/07/21 12:00:00 prueba COLIMA canal 2",
            sm,
            telegram_id=2001,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.SINGLE
        assert result.canal is None

    def test_single_fallback(self):
        sm = _make_station_manager(
            phone_map={"5561048913": ["PC Colima"]},
            open_close={"PC Colima": ("COLIMA SUP", "Restablecimiento")},
        )
        result = parse(
            "+5561048913 17/03/2026 10:30:00 random text that matches nothing",
            sm,
            telegram_id=1004,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.SINGLE

    def test_unknown_phone(self):
        sm = _make_station_manager()  # empty
        result = parse(
            "+9999999999 17/03/2026 08:00:00 some content",
            sm,
            telegram_id=1005,
        )
        assert result is not None
        assert result.estacion == "Estacion 9999999999"
        assert result.tipo_mensaje == MessageType.SINGLE

    def test_returns_none_for_garbage(self):
        sm = _make_station_manager()
        assert parse("not a valid message at all", sm) is None
        assert parse("", sm) is None
        assert parse(None, sm) is None

    def test_comma_separated_open(self):
        """Open text with multiple comma-separated patterns (e.g. SICOM Puebla)."""
        sm = _make_station_manager(
            phone_map={"5529078334": ["SICOM Puebla"]},
            open_close={"SICOM Puebla": ("Puebla ALT,Puebla SUP", "Puebla SUP")},
        )
        result = parse(
            "+5529078334 17/03/2026 08:45:00 Puebla ALT",
            sm,
            telegram_id=1006,
        )
        assert result is not None
        assert result.tipo_mensaje == MessageType.OPEN

    def test_country_code_stripping(self):
        sm = _make_station_manager(phone_map={"5561048913": ["PC Colima"]})
        result = parse(
            "+525561048913 17/03/2026 08:00:00 test",
            sm,
            telegram_id=1007,
        )
        assert result is not None
        assert result.telefono == "5561048913"
