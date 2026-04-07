"""
schedule_engine.py - Tono window calculation.

Determines whether a message's internal timestamp falls inside a
forward-only tono window.

Main windows: {02:45, 05:45, 08:45, 11:45, 14:45, 17:45, 20:45, 23:45}

A message is tono when its timestamp falls within:
    [window_start, window_start + TONO_WINDOW_SECONDS]
"""

from __future__ import annotations

from datetime import datetime, time, timedelta

from config import REPORT_HOURS, REPORT_MINUTE, TONO_WINDOW_SECONDS


def _build_windows() -> list[time]:
    """Build the ordered list of tono target times for a single day."""
    windows = [time(hour, REPORT_MINUTE, 0) for hour in REPORT_HOURS]
    windows.sort()
    return windows


TONO_WINDOWS: list[time] = _build_windows()


def is_tono(msg_time: datetime) -> bool:
    """
    Check whether *msg_time* falls within any forward-only tono window.

    Uses the message's internal timestamp, not the reception time.
    """
    window_delta = timedelta(seconds=TONO_WINDOW_SECONDS)

    for window_time in TONO_WINDOWS:
        window_start = datetime.combine(msg_time.date(), window_time)
        window_end = window_start + window_delta
        if window_start <= msg_time <= window_end:
            return True

    return False


def get_window_range(msg_time: datetime) -> tuple[datetime, datetime] | None:
    """Return the matching tono window bounds, or None when outside the window."""
    window_delta = timedelta(seconds=TONO_WINDOW_SECONDS)

    for window_time in TONO_WINDOWS:
        window_start = datetime.combine(msg_time.date(), window_time)
        window_end = window_start + window_delta
        if window_start <= msg_time <= window_end:
            return window_start, window_end

    return None


def get_active_windows() -> list[time]:
    """Return the precomputed tono windows."""
    return list(TONO_WINDOWS)
