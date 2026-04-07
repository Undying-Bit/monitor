"""
telegram_client.py - Telethon-based ingress service.

Connects to Telegram via MTProto, listens for new messages on the
configured group, and pushes RawEvent objects into an asyncio.Queue.
On startup, replays unseen Telegram history using a persisted watermark.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

from telethon import TelegramClient, events

import persistence
from config import (
    API_ID,
    API_HASH,
    GROUP_ID,
    SESSION_NAME,
    TELEGRAM_CATCHUP_OVERLAP_MESSAGES,
    TELEGRAM_TIMEOUT_SECONDS,
    TELEGRAM_REQUEST_RETRIES,
    TELEGRAM_CONNECTION_RETRIES,
    TELEGRAM_RETRY_DELAY_SECONDS,
)
from models import RawEvent, Source

logger = logging.getLogger(__name__)


class TelegramIngress:
    """Manages the Telethon connection and feeds messages into a queue."""

    def __init__(self, queue: asyncio.Queue[RawEvent]) -> None:
        self._queue = queue
        self._client: TelegramClient | None = None

    async def start(self) -> None:
        """Connect, register the live handler, then run catch-up replay."""
        self._client = TelegramClient(
            SESSION_NAME,
            API_ID,
            API_HASH,
            timeout=TELEGRAM_TIMEOUT_SECONDS,
            request_retries=TELEGRAM_REQUEST_RETRIES,
            connection_retries=TELEGRAM_CONNECTION_RETRIES,
            retry_delay=TELEGRAM_RETRY_DELAY_SECONDS,
            auto_reconnect=True,
        )
        await self._client.start()

        me = await self._client.get_me()
        logger.info(
            "Telegram connected as: %s (id=%s)",
            f"{me.first_name} {me.last_name or ''}".strip(),
            me.id,
        )

        self._register_live_handler()
        await self._catchup()

    def _register_live_handler(self) -> None:
        """Register the live Telegram message handler before catch-up begins."""
        if self._client is None:
            raise RuntimeError("Telegram client not initialised")

        @self._client.on(events.NewMessage(chats=GROUP_ID))
        async def _on_new_message(event):
            raw = self._message_to_raw_event(event.message)
            if raw is None:
                return
            await self._queue.put(raw)
            logger.debug("Queued live msg id=%s", raw.source_event_id)

        logger.info("Live message handler registered for group %d", GROUP_ID)

    def _message_to_raw_event(self, message: Any) -> RawEvent | None:
        """Convert a Telethon message into a RawEvent when it has text."""
        text = getattr(message, "text", None)
        message_id = getattr(message, "id", None)
        if not text or message_id is None:
            return None

        return RawEvent(
            source=Source.TELEGRAM,
            source_event_id=str(message_id),
            raw_payload=text,
            received_at=datetime.utcnow(),
            transport_meta={"group_id": GROUP_ID},
        )

    async def _catchup(self) -> None:
        """Replay unseen history, with a small overlap for startup safety."""
        if self._client is None:
            raise RuntimeError("Telegram client not initialised")

        queued_count = 0
        last_seen_id = await persistence.get_latest_telegram_message_id()
        overlap = max(0, TELEGRAM_CATCHUP_OVERLAP_MESSAGES)
        iter_kwargs: dict[str, Any] = {"reverse": True}

        if last_seen_id is None:
            logger.info(
                "Catch-up bootstrap: no Telegram watermark found; replaying full history"
            )
        else:
            min_id = max(0, last_seen_id - overlap)
            iter_kwargs["min_id"] = min_id
            logger.info(
                "Catch-up replay: watermark=%d overlap=%d min_id=%d",
                last_seen_id,
                overlap,
                min_id,
            )

        try:
            from telethon.errors.rpcerrorlist import BotMethodInvalidError

            async for message in self._client.iter_messages(
                GROUP_ID,
                **iter_kwargs,
            ):
                raw = self._message_to_raw_event(message)
                if raw is None:
                    continue
                await self._queue.put(raw)
                queued_count += 1

            logger.info("Catch-up complete: %d messages queued", queued_count)
        except BotMethodInvalidError as e:
            logger.warning(
                "Catch-up disabled: Bots cannot fetch history for this chat (%s)",
                e,
            )
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
