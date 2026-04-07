"""
orchestrator.py - Pipe-and-filter pipeline for ingestion.

Consumes RawEvent objects and pushes them through:
Parse -> Normalize -> Persist -> Resolve.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

from models import NormalizedEvent, RawEvent, Source
from normalization import normalize_serial, normalize_telegram
from parser_engine import parse_telegram
from resolution_engine import resolve_slot
from schedule_engine import get_window_range
from serial_parser import parse_serial_payload
from station_manager import StationManager
from station_resolver import StationResolver
import persistence

logger = logging.getLogger(__name__)


class Orchestrator:
    """Consumes raw events from the queue and processes them end-to-end."""

    def __init__(
        self,
        queue: asyncio.Queue[RawEvent],
        station_manager: StationManager,
    ) -> None:
        self._queue = queue
        self._station_mgr = station_manager
        self._resolver = StationResolver(station_manager)

    async def run(self) -> None:
        """Infinite consumer loop."""
        logger.info("Orchestrator consumer started")

        while True:
            raw: RawEvent = await self._queue.get()

            try:
                await self._process(raw)
            except Exception as exc:
                logger.error(
                    "Unhandled error processing source=%s: %s",
                    raw.source.value,
                    exc,
                    exc_info=True,
                )
            finally:
                self._queue.task_done()

    async def _process(self, raw: RawEvent) -> None:
        inserted, raw_event_id = await persistence.insert_raw_event(raw)
        if not inserted:
            logger.info(
                "Deduped raw event source=%s source_event_id=%s",
                raw.source.value,
                raw.source_event_id,
            )
            return

        meta = raw.transport_meta or {}

        logger.info(
            "Stored raw event id=%s source=%s",
            raw_event_id,
            raw.source.value,
        )

        normalized: list[NormalizedEvent] = []
        parse_error: Optional[str] = None

        if raw.source == Source.TELEGRAM:
            parsed = parse_telegram(raw.raw_payload)
            if not parsed:
                parse_error = "telegram_parse_failed"
            else:
                logger.debug("Parsed telegram payload raw_event_id=%s", raw_event_id)
                norm = normalize_telegram(
                    parsed,
                    raw_event_id=raw_event_id,
                    station_manager=self._station_mgr,
                    resolver=self._resolver,
                )
                # Keep STATE rows individually valid; only suppress non-state duplicates.
                if (
                    norm.event_class != "STATE"
                    and norm.tone
                    and norm.report_slot
                    and norm.station_id
                ):
                    window = get_window_range(parsed.timestamp)
                    if window:
                        start, end = window
                        if await persistence.has_open_and_close(
                            norm.station_id, start, end
                        ):
                            norm.tone = False
                            norm.is_valid_report = False
                normalized = [norm]

        elif raw.source == Source.SERIAL:
            parsed_list = parse_serial_payload(raw.raw_payload)
            if not parsed_list:
                parse_error = "serial_parse_failed"
            else:
                logger.debug(
                    "Parsed serial payload raw_event_id=%s headers=%d",
                    raw_event_id,
                    len(parsed_list),
                )
                for parsed in parsed_list:
                    norm = normalize_serial(
                        parsed,
                        raw_event_id=raw_event_id,
                        resolver=self._resolver,
                        received_at=raw.received_at,
                    )
                    normalized.append(norm)

        else:
            parse_error = f"unknown_source:{raw.source}"

        if not normalized:
            status = "error"
            if raw.source == Source.SERIAL and meta.get("frame_overflow"):
                status = "partial"
                parse_error = "serial_frame_overflow"
                logger.warning(
                    "Serial frame overflow raw_event_id=%s (stored as partial)",
                    raw_event_id,
                )
            await persistence.update_raw_event_status(
                raw_event_id, status, parse_error
            )
            logger.warning(
                "Parse failed raw_event_id=%s source=%s error=%s",
                raw_event_id,
                raw.source.value,
                parse_error,
            )
            return

        if raw.source == Source.SERIAL and meta.get("frame_overflow"):
            await persistence.update_raw_event_status(
                raw_event_id, "partial", "serial_frame_overflow"
            )
        else:
            await persistence.update_raw_event_status(raw_event_id, "parsed", None)

        for norm in normalized:
            norm_id = await persistence.insert_normalized_event(norm)
            logger.info(
                "Normalized event id=%s raw_event_id=%s type=%s slot=%s valid=%s",
                norm_id,
                raw_event_id,
                norm.event_type,
                norm.report_slot,
                norm.is_valid_report,
            )
            if (
                norm.station_id
                and norm.report_date
                and norm.report_slot
                and norm.event_type == "RWT"
                and norm.tone
            ):
                await persistence.enforce_first_valid_for_slot(
                    norm.station_id,
                    norm.report_date,
                    norm.report_slot,
                    norm.event_type,
                )
            if norm.station_id and norm.report_date and norm.report_slot:
                await resolve_slot(norm.station_id, norm.report_date, norm.report_slot)
