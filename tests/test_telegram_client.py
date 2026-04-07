import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import telegram_client


class FakeTelegramClient:
    def __init__(self, messages):
        self._messages = list(messages)
        self.handlers = []
        self.iter_calls = []
        self.handler_count_when_iter_started = None

    async def start(self):
        return None

    async def get_me(self):
        return SimpleNamespace(first_name="Test", last_name=None, id=1)

    def on(self, event):
        def decorator(func):
            self.handlers.append((event, func))
            return func

        return decorator

    def iter_messages(self, entity, **kwargs):
        self.iter_calls.append((entity, kwargs))
        self.handler_count_when_iter_started = len(self.handlers)

        async def _generator():
            for message in self._messages:
                yield message

        return _generator()

    async def run_until_disconnected(self):
        return None

    async def disconnect(self):
        return None


async def _drain_ids(queue: asyncio.Queue) -> list[str]:
    ids = []
    while not queue.empty():
        ids.append((await queue.get()).source_event_id)
        queue.task_done()
    return ids


@pytest.mark.asyncio
async def test_start_registers_live_handler_before_catchup_and_replays_overlap(
    monkeypatch,
):
    queue = asyncio.Queue()
    fake_client = FakeTelegramClient(
        [
            SimpleNamespace(id=181, text="first unseen"),
            SimpleNamespace(id=182, text=""),
            SimpleNamespace(id=205, text="second unseen"),
        ]
    )

    monkeypatch.setattr(telegram_client, "TelegramClient", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr(
        telegram_client.persistence,
        "get_latest_telegram_message_id",
        AsyncMock(return_value=200),
    )
    monkeypatch.setattr(telegram_client, "GROUP_ID", 12345)
    monkeypatch.setattr(telegram_client, "TELEGRAM_CATCHUP_OVERLAP_MESSAGES", 20)

    ingress = telegram_client.TelegramIngress(queue)
    await ingress.start()

    assert fake_client.handler_count_when_iter_started == 1
    assert len(fake_client.handlers) == 1
    assert fake_client.iter_calls == [
        (12345, {"reverse": True, "min_id": 180}),
    ]
    assert await _drain_ids(queue) == ["181", "205"]


@pytest.mark.asyncio
async def test_catchup_without_watermark_replays_full_history(monkeypatch):
    queue = asyncio.Queue()
    fake_client = FakeTelegramClient(
        [
            SimpleNamespace(id=1, text="bootstrap one"),
            SimpleNamespace(id=2, text=None),
            SimpleNamespace(id=3, text="bootstrap two"),
        ]
    )

    monkeypatch.setattr(telegram_client, "TelegramClient", lambda *args, **kwargs: fake_client)
    monkeypatch.setattr(
        telegram_client.persistence,
        "get_latest_telegram_message_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(telegram_client, "GROUP_ID", 67890)

    ingress = telegram_client.TelegramIngress(queue)
    await ingress.start()

    assert fake_client.iter_calls == [
        (67890, {"reverse": True}),
    ]
    assert await _drain_ids(queue) == ["1", "3"]
