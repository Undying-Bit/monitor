import asyncio
from unittest.mock import AsyncMock

import pytest

import main as main_module
import orchestrator
import persistence
import station_manager
import telegram_client


def test_parse_args_accepts_positive_catchup_overlap():
    args = main_module.parse_args(["--telegram-catchup-overlap-messages", "25"])
    assert args.telegram_catchup_overlap_messages == 25


def test_parse_args_accepts_positive_catchup_last_days():
    args = main_module.parse_args(["--telegram-catchup-last-days", "7"])
    assert args.telegram_catchup_last_days == 7


def test_parse_args_rejects_non_positive_catchup_overlap():
    with pytest.raises(SystemExit):
        main_module.parse_args(["--telegram-catchup-overlap-messages", "0"])


def test_parse_args_rejects_non_positive_catchup_last_days():
    with pytest.raises(SystemExit):
        main_module.parse_args(["--telegram-catchup-last-days", "0"])


@pytest.mark.asyncio
async def test_main_threads_catchup_overlap_into_telegram_ingress(monkeypatch):
    captured: dict[str, object] = {}

    class FakeOrchestrator:
        def __init__(self, queue, station_mgr):
            captured["queue"] = queue
            captured["station_mgr"] = station_mgr

        async def run(self):
            await asyncio.sleep(0)

    class FakeTelegramIngress:
        def __init__(
            self,
            queue,
            *,
            catchup_overlap_messages=None,
            catchup_last_days=None,
        ):
            captured["ingress_queue"] = queue
            captured["catchup_overlap_messages"] = catchup_overlap_messages
            captured["catchup_last_days"] = catchup_last_days

        async def start(self):
            return None

        async def run_forever(self):
            raise KeyboardInterrupt

        async def stop(self):
            return None

    monkeypatch.setattr(persistence, "init_db", AsyncMock(return_value=None))
    monkeypatch.setattr(station_manager, "StationManager", lambda: object())
    monkeypatch.setattr(orchestrator, "Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(telegram_client, "TelegramIngress", FakeTelegramIngress)

    await main_module.main(["--telegram-catchup-overlap-messages", "40"])

    assert captured["catchup_overlap_messages"] == 40
    assert captured["catchup_last_days"] is None
    assert captured["ingress_queue"] is captured["queue"]


@pytest.mark.asyncio
async def test_main_threads_catchup_last_days_into_telegram_ingress(monkeypatch):
    captured: dict[str, object] = {}

    class FakeOrchestrator:
        def __init__(self, queue, station_mgr):
            captured["queue"] = queue
            captured["station_mgr"] = station_mgr

        async def run(self):
            await asyncio.sleep(0)

    class FakeTelegramIngress:
        def __init__(
            self,
            queue,
            *,
            catchup_overlap_messages=None,
            catchup_last_days=None,
        ):
            captured["ingress_queue"] = queue
            captured["catchup_overlap_messages"] = catchup_overlap_messages
            captured["catchup_last_days"] = catchup_last_days

        async def start(self):
            return None

        async def run_forever(self):
            raise KeyboardInterrupt

        async def stop(self):
            return None

    monkeypatch.setattr(persistence, "init_db", AsyncMock(return_value=None))
    monkeypatch.setattr(station_manager, "StationManager", lambda: object())
    monkeypatch.setattr(orchestrator, "Orchestrator", FakeOrchestrator)
    monkeypatch.setattr(telegram_client, "TelegramIngress", FakeTelegramIngress)

    await main_module.main(["--telegram-catchup-last-days", "7"])

    assert captured["catchup_overlap_messages"] is None
    assert captured["catchup_last_days"] == 7
    assert captured["ingress_queue"] is captured["queue"]
