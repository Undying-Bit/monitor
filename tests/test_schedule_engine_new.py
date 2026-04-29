from datetime import datetime, timedelta

from schedule_engine import TONO_WINDOW_SECONDS, get_window_range, is_tono


def test_get_window_range_success():
    msg_time = datetime(2026, 3, 18, 5, 45, 50)
    range_val = get_window_range(msg_time)
    assert range_val is not None

    start, end = range_val
    assert start == datetime(2026, 3, 18, 5, 45, 0)
    assert end == start + timedelta(seconds=TONO_WINDOW_SECONDS)


def test_get_window_range_fail():
    msg_time = datetime(2026, 3, 18, 5, 40, 0)
    assert get_window_range(msg_time) is None


def test_forward_only_tono_boundaries():
    assert is_tono(datetime(2026, 3, 18, 5, 44, 59)) is False
    assert is_tono(datetime(2026, 3, 18, 5, 45, 0)) is True
    assert is_tono(datetime(2026, 3, 18, 5, 47, 0)) is True
    assert is_tono(datetime(2026, 3, 18, 5, 47, 1)) is False
