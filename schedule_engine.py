"""
schedule_engine.py — Tono window calculation.

Determines whether a message's internal timestamp falls inside a
"tono" (on-time) broadcast window.

Main windows:   {02:45, 05:45, 08:45, 11:45, 14:45, 17:45, 20:45, 23:45}

A message is "tono" if its timestamp falls within
[window_start, window_start + TONO_WINDOW_SECONDS].
"""
from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import List

from config import REPORT_HOURS, REPORT_MINUTE, TONO_WINDOW_SECONDS

def _build_windows() -> list[time]:
    """
    Build the ordered list of tono target times for a single day.

    Returns times as datetime.time objects.
    """
    windows: list[time] = []

    # Main windows: REPORT_HOURS at :45
    for h in REPORT_HOURS:
        windows.append(time(h, REPORT_MINUTE, 0))

    windows.sort()
    return windows


# Pre-built for fast access
TONO_WINDOWS: list[time] = _build_windows()


def is_tono(msg_time: datetime) -> bool:
    """
    Check whether *msg_time* falls within any tono window.

    Uses the message's internal timestamp (not reception time).
    Window = [target, target + TONO_WINDOW_SECONDS].
    """
    window_delta = timedelta(seconds=TONO_WINDOW_SECONDS)

    for w in TONO_WINDOWS:
        # Build a full datetime for the window on the same date as msg_time
        window_start = datetime.combine(msg_time.date(), w)
        window_end = window_start + window_delta

        if window_start <= msg_time <= window_end:
            return True

    # Also check the last window of the *previous* day that might bleed over
    # (e.g., 23:45 window + 2 min → spans into 23:47, same day — no issue)
    # And the first window of the *next* day for messages near midnight
    # (edge case: message at 00:00:01 could match a 00:00 window if 0 not skipped)
    return False


def get_active_windows() -> list[time]:
    """Return the pre-computed list of tono windows (for inspection/testing)."""
    return list(TONO_WINDOWS)
