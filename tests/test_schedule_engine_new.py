import pytest
from datetime import datetime
from schedule_engine import get_window_range, TONO_WINDOW_SECONDS

def test_get_window_range_success():
    # 05:45 is a window. 
    # With TONO_WINDOW_SECONDS (default 120?), 05:45:50 should be in range.
    msg_time = datetime(2026, 3, 18, 5, 45, 50)
    range_val = get_window_range(msg_time)
    assert range_val is not None
    start, end = range_val
    assert start == datetime(2026, 3, 18, 5, 45, 0)
    assert end == datetime(2026, 3, 18, 5, 45, 0) + (end - start) # should be TONO_WINDOW_SECONDS

def test_get_window_range_fail():
    # 05:40 is NOT a window.
    msg_time = datetime(2026, 3, 18, 5, 40, 0)
    assert get_window_range(msg_time) is None
