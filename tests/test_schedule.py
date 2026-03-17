"""
tests/test_schedule.py — Unit tests for the ScheduleEngine.

Uses freezegun to mock datetime and verify tono window calculations.
Covers:
  - Main windows (HH:45 for report hours)
  - Hourly windows (H:00, excluding skipped ones)
  - Boundary conditions (just inside / just outside 2-minute window)
  - Skip logic verification
"""
import pytest
from datetime import datetime

# Ensure project root is on path
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from schedule_engine import is_tono, get_active_windows
from config import REPORT_HOURS


class TestTonoMainWindows:
    """Test the 3-hour main windows at HH:45."""

    @pytest.mark.parametrize("hour", REPORT_HOURS)
    def test_exact_start_is_tono(self, hour):
        """Message at exactly HH:45:00 → tono."""
        t = datetime(2026, 3, 17, hour, 45, 0)
        assert is_tono(t) is True

    @pytest.mark.parametrize("hour", REPORT_HOURS)
    def test_within_window_is_tono(self, hour):
        """Message at HH:45:30 (30s in) → tono."""
        t = datetime(2026, 3, 17, hour, 45, 30)
        assert is_tono(t) is True

    @pytest.mark.parametrize("hour", REPORT_HOURS)
    def test_end_boundary_is_tono(self, hour):
        """Message at HH:47:00 (exactly 120s) → tono."""
        t = datetime(2026, 3, 17, hour, 47, 0)
        assert is_tono(t) is True

    @pytest.mark.parametrize("hour", REPORT_HOURS)
    def test_just_after_window_not_tono(self, hour):
        """Message at HH:47:01 (121s) → NOT tono."""
        t = datetime(2026, 3, 17, hour, 47, 1)
        assert is_tono(t) is False

    @pytest.mark.parametrize("hour", REPORT_HOURS)
    def test_just_before_window_not_tono(self, hour):
        """Message at HH:44:59 → NOT tono."""
        t = datetime(2026, 3, 17, hour, 44, 59)
        assert is_tono(t) is False


class TestHourlyWindowsAlwaysFalse:
    """Verify that any hourly window (:00) is NOT considered tono."""

    def test_top_of_hour_is_not_tono(self):
        """04:00:00 -> NOT tono."""
        t = datetime(2026, 3, 17, 4, 0, 0)
        assert is_tono(t) is False

    def test_skipped_hour_is_not_tono(self):
        """03:00:00 -> NOT tono."""
        t = datetime(2026, 3, 17, 3, 0, 0)
        assert is_tono(t) is False


class TestEdgeCases:
    """Boundary and edge-case scenarios."""

    def test_random_time_not_tono(self):
        """A message at 10:30 should not be tono (no window near)."""
        t = datetime(2026, 3, 17, 10, 30, 0)
        assert is_tono(t) is False

    def test_noon_not_tono(self):
        """12:00 is not a main window -> False."""
        t = datetime(2026, 3, 17, 12, 0, 0)
        assert is_tono(t) is False

    def test_windows_are_sorted(self):
        """The pre-computed window list should be ascending."""
        windows = get_active_windows()
        for i in range(len(windows) - 1):
            assert windows[i] <= windows[i + 1]

    def test_1am_is_not_tono(self):
        """01:00 is not a main window -> False."""
        t = datetime(2026, 3, 17, 1, 0, 0)
        assert is_tono(t) is False

    def test_midnight_is_not_tono(self):
        """00:00 is not a main window -> False."""
        t = datetime(2026, 3, 17, 0, 0, 0)
        assert is_tono(t) is False
