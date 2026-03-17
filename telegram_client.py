"""
telegram_client.py — Telethon-based ingress service.

Connects to Telegram via MTProto, listens for new messages on the
configured group, and pushes RawMessage objects into an asyncio.Queue.
On startup, performs a catch-up scan of the last CATCHUP_LIMIT messages.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from telethon import TelegramClient, events

from config import API_ID, API_HASH, GROUP_ID, SESSION_NAME, CATCHUP_LIMIT
from models import RawMessage

logger = logging.getLogger(__name__)


class TelegramIngress:
    """Manages the Telethon connection and feeds messages into a queue."""

    def __init__(self, queue: asyncio.Queue[RawMessage]) -> None:
        self._queue = queue
        self._client: TelegramClient | None = None

    async def start(self) -> None:
        """Connect, do catch-up scan, then register live handler."""
        self._client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        await self._client.start()

        me = await self._client.get_me()
        logger.info("Telegram connected as: %s (id=%s)", me.username, me.id)

        # ── Catch-up scan ────────────────────────────────────
        await self._catchup()

        # ── Live handler ─────────────────────────────────────
        @self._client.on(events.NewMessage(chats=GROUP_ID))
        async def _on_new_message(event):
            text = event.message.text
            if not text:
                return
            raw = RawMessage(
                telegram_id=event.message.id,
                raw_text=text,
                receive_timestamp=datetime.now(),
            )
            await self._queue.put(raw)
            logger.debug("Queued live msg id=%d", raw.telegram_id)

        logger.info("Live message handler registered for group %d", GROUP_ID)

    async def _catchup(self) -> None:
        """Scan the last N messages to fill gaps from downtime."""
        logger.info("Catch-up scan: fetching last %d messages…", CATCHUP_LIMIT)
        count = 0
        try:
            from telethon.errors.rpcerrorlist import BotMethodInvalidError
            async for message in self._client.iter_messages(
                GROUP_ID, limit=CATCHUP_LIMIT
            ):
                if not message.text:
                    continue
                raw = RawMessage(
                    telegram_id=message.id,
                    raw_text=message.text,
                    receive_timestamp=datetime.now(),
                )
                await self._queue.put(raw)
                count += 1
            logger.info("Catch-up complete: %d messages queued", count)
        except BotMethodInvalidError as e:
            logger.warning("Catch-up disabled: Bots cannot fetch history for this chat (%s)", e)
        except Exception as e:
            logger.error("Catch-up scan failed: %s", e)

    async def run_forever(self) -> None:
        """Keep the client running (blocks until disconnected)."""
        if self._client:
            await self._client.run_until_disconnected()

    async def stop(self) -> None:
        """Gracefully disconnect the Telegram client."""
        if self._client:
            await self._client.disconnect()
            logger.info("Telegram client disconnected")
