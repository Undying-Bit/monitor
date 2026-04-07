import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from serial_parser import parse_serial_payload


def test_parse_serial_rwt_header():
    raw = "ZCZC-CIV-RWT-009000-015000+0300-1221200-XCMX/011-"
    parsed = parse_serial_payload(raw)
    assert len(parsed) == 1
    p = parsed[0]
    assert p.originator == "CIV"
    assert p.event_code == "RWT"
    assert p.area_codes == ["009000", "015000"]
    assert p.duration_code == "0300"
    assert p.julian_day == 122
    assert p.hour == 12
    assert p.minute == 0
    assert p.transmitter_code == "XCMX/011"


def test_parse_serial_repeats_counted():
    header = "ZCZC-CIV-EQW-000000+0001-1221200-XCMX/011-"
    raw = f"{header} {header} {header}"
    parsed = parse_serial_payload(raw)
    assert len(parsed) == 1
    assert parsed[0].repeat_count == 3
