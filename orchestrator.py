"""
orchestrator.py — Pipe-and-filter pipeline that wires all modules.

Consumes RawMessage objects from the asyncio.Queue and pushes them
through: ParserEngine → StationManager → ScheduleEngine → Persistence.
"""
from __future__ import annotations

import asyncio
import logging

from models import RawMessage
from parser_engine import parse
import schedule_engine
from station_manager import StationManager
import persistence

logger = logging.getLogger(__name__)


class Orchestrator:
    """Consumes raw messages from the queue and processes them end-to-end."""

    def __init__(
        self,
        queue: asyncio.Queue[RawMessage],
        station_manager: StationManager,
    ) -> None:
        self._queue = queue
        self._station_mgr = station_manager

    async def run(self) -> None:
        """
        Infinite consumer loop.

        Drains the queue one message at a time, running the full
        parse → identify → classify → tono → persist pipeline.
        """
        logger.info("Orchestrator consumer started")

        while True:
            raw: RawMessage = await self._queue.get()

            try:
                await self._process(raw)
            except Exception as exc:
                logger.error(
                    "Unhandled error processing msg id=%d: %s",
                    raw.telegram_id,
                    exc,
                    exc_info=True,
                )
            finally:
                self._queue.task_done()

    async def _process(self, raw: RawMessage) -> None:
        """Run the full pipeline for a single RawMessage."""

        # 1. Check idempotency early (cheap DB lookup)
        if await persistence.message_exists(raw.telegram_id):
            logger.debug("Skipping duplicate telegram_id=%d", raw.telegram_id)
            return

        # 2. Parse
        parsed = parse(
            raw.raw_text,
            station_manager=self._station_mgr,
            telegram_id=raw.telegram_id,
        )
        if parsed is None:
            logger.debug(
                "Unparseable message id=%d: %s",
                raw.telegram_id,
                raw.raw_text[:80],
            )
            return

        # 3. Temporal analysis — tono
        if not self._station_mgr.lookup_by_phone(parsed.telefono):
            logger.info(
                "Skipping unregistered phone %s (telegram_id=%d)",
                parsed.telefono,
                raw.telegram_id,
            )
            return

        window_range = schedule_engine.get_window_range(parsed.timestamp)
        parsed.tono = window_range is not None

        # 3.1. Tone logic supplement: If already have OPEN and CLOSE in window, third is not tono
        if parsed.tono and window_range:
            start, end = window_range
            if await persistence.has_open_and_close(parsed.estacion, start, end):
                parsed.tono = False

        # 4. Persist
        inserted = await persistence.insert_message(parsed)

        if inserted:
            tono_tag = " [TONO]" if parsed.tono else ""
            logger.info(
                "Stored: %s (%s) | %s | %s%s",
                parsed.estacion,
                parsed.red,
                parsed.tipo_mensaje.value,
                parsed.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                tono_tag,
            )
