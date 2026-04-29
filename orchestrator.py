"""
orchestrator.py - Queue consumer for transactional raw event processing.
"""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from models import RawEvent
from processing_service import ProcessingService
from station_manager import StationManager

logger = logging.getLogger(__name__)


class Orchestrator:
    """Consumes raw events from the queue and processes them end-to-end."""

    def __init__(
        self,
        queue: asyncio.Queue[RawEvent],
        station_manager: StationManager,
        db_path: Path | None = None,
    ) -> None:
        self._queue = queue
        self._processor = ProcessingService(station_manager, db_path=db_path)

    async def run(self) -> None:
        """Infinite consumer loop."""
        await self._processor.start()
        logger.info("Orchestrator consumer started")

        try:
            while True:
                raw: RawEvent = await self._queue.get()

                try:
                    await self._process(raw)
                except Exception as exc:
                    logger.error(
                        "Unhandled error processing source=%s source_event_id=%s: %s",
                        raw.source.value,
                        raw.source_event_id,
                        exc,
                        exc_info=True,
                    )
                finally:
                    self._queue.task_done()
        finally:
            await self._processor.close()

    async def _process(self, raw: RawEvent) -> None:
        result = await self._processor.process_new_raw(raw)
        if result.deduped:
            logger.info(
                "Deduped raw event source=%s source_event_id=%s",
                raw.source.value,
                raw.source_event_id,
            )
            return

        logger.info(
            "Processed raw event id=%s source=%s status=%s normalized=%d",
            result.raw_event_id,
            raw.source.value,
            result.status,
            result.normalized_count,
        )
