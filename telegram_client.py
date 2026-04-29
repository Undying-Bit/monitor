"""
telegram_client.py - Telethon-based ingress service.

Connects to Telegram via MTProto, listens for new messages on the
configured group, and pushes RawEvent objects into an asyncio.Queue.
On startup, replays unseen Telegram history using a persisted watermark.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
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

    def __init__(
        self,
        queue: asyncio.Queue[RawEvent],
        *,
        catchup_overlap_messages: int | None = None,
        catchup_last_days: int | None = None,
    ) -> None:
        if catchup_overlap_messages is not None and catchup_overlap_messages <= 0:
            raise ValueError("catchup_overlap_messages must be positive when provided")
        if catchup_last_days is not None and catchup_last_days <= 0:
            raise ValueError("catchup_last_days must be positive when provided")
        self._queue = queue
        self._client: TelegramClient | None = None
        self._last_backpressure_log = 0.0
        self._catchup_overlap_messages = catchup_overlap_messages
        self._catchup_last_days = catchup_last_days

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
            self._maybe_log_queue_pressure("live")
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
            received_at=datetime.now(timezone.utc),
            transport_meta={"group_id": GROUP_ID},
        )

    async def _catchup(self) -> None:
        """Replay unseen history, with a small overlap for startup safety."""
        if self._client is None:
            raise RuntimeError("Telegram client not initialised")

        if self._catchup_last_days is not None:
            await self._catchup_recent_days(self._catchup_last_days)
            return

        queued_count = 0
        last_seen_id = await persistence.get_latest_telegram_message_id()
        configured_overlap = max(0, TELEGRAM_CATCHUP_OVERLAP_MESSAGES)
        overlap = configured_overlap
        if self._catchup_overlap_messages is not None:
            overlap = max(configured_overlap, self._catchup_overlap_messages)
        iter_kwargs: dict[str, Any] = {"reverse": True}

        if last_seen_id is None:
            logger.info(
                "Catch-up bootstrap: no Telegram watermark found; replaying full history"
            )
        else:
            min_id = max(0, last_seen_id - overlap)
            iter_kwargs["min_id"] = min_id
            logger.info(
                "Catch-up replay: watermark=%d configured_overlap=%d overlap=%d min_id=%d",
                last_seen_id,
                configured_overlap,
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
                self._maybe_log_queue_pressure("catchup")
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

    async def _catchup_recent_days(self, catchup_last_days: int) -> None:
        """Replay recent history by message date, ignoring the stored watermark."""
        if self._client is None:
            raise RuntimeError("Telegram client not initialised")

        cutoff_utc = datetime.now(timezone.utc) - timedelta(days=catchup_last_days)
        buffered: list[RawEvent] = []
        logger.info(
            "Catch-up replay override: ignoring watermark and replaying messages since %s",
            cutoff_utc.isoformat(),
        )

        try:
            from telethon.errors.rpcerrorlist import BotMethodInvalidError

            async for message in self._client.iter_messages(GROUP_ID):
                message_date = self._message_datetime_utc(message)
                if message_date is not None and message_date < cutoff_utc:
                    break

                raw = self._message_to_raw_event(message)
                if raw is None:
                    continue
                buffered.append(raw)

            for raw in reversed(buffered):
                self._maybe_log_queue_pressure("catchup-last-days")
                await self._queue.put(raw)

            logger.info(
                "Catch-up last-days complete: %d messages queued from the last %d day(s)",
                len(buffered),
                catchup_last_days,
            )
        except BotMethodInvalidError as e:
            logger.warning(
                "Catch-up disabled: Bots cannot fetch history for this chat (%s)",
                e,
            )
        except Exception as e:
            logger.error("Catch-up scan failed: %s", e)

    def _message_datetime_utc(self, message: Any) -> datetime | None:
        message_date = getattr(message, "date", None)
        if message_date is None:
            return None
        if message_date.tzinfo is None:
            return message_date.replace(tzinfo=timezone.utc)
        return message_date.astimezone(timezone.utc)

    async def run_forever(self) -> None:
        """Keep the client running (blocks until disconnected)."""
        if self._client:
            await self._client.run_until_disconnected()

    async def stop(self) -> None:
        """Gracefully disconnect the Telegram client."""
        if self._client:
            await self._client.disconnect()
            logger.info("Telegram client disconnected")

    def _maybe_log_queue_pressure(self, context: str) -> None:
        maxsize = self._queue.maxsize
        if maxsize <= 0:
            return
        qsize = self._queue.qsize()
        if qsize < max(1, int(maxsize * 0.8)):
            return
        now = time.monotonic()
        if now - self._last_backpressure_log < 5.0:
            return
        self._last_backpressure_log = now
        logger.warning(
            "Telegram queue pressure during %s: %d/%d buffered",
            context,
            qsize,
            maxsize,
        )
