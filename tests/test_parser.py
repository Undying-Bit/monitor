"""
tests/test_parser.py - Unit tests for Telegram parsing.
"""
import sys, os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from parser_engine import parse_telegram, TIER1_RE, TIER2_RE


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

    def test_no_match_missing_time(self):
        text = "+5561048913 17/03/2026 COLIMA SUP"
        assert TIER1_RE.match(text) is None


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


class TestParseTelegram:
    def test_parse_open_like_message(self):
        parsed = parse_telegram("+5561048913 17/03/2026 14:45:00 COLIMA SUP")
        assert parsed is not None
        assert parsed.phone == "5561048913"
        assert parsed.timestamp.hour == 14
        assert parsed.content == "COLIMA SUP"

    def test_parse_mensaje_with_hint(self):
        parsed = parse_telegram(
            "+5561048913 17/03/2026 12:00:00 MENSAJE **/07/21 12:00:00 prueba COLIMA canal 2"
        )
        assert parsed is not None
        assert parsed.is_mensaje is True
        assert parsed.mensaje_station_hint == "COLIMA"
        assert parsed.mensaje_channel_raw == "canal 2"

    def test_returns_none_for_garbage(self):
        assert parse_telegram("not a valid message at all") is None
        assert parse_telegram("") is None
