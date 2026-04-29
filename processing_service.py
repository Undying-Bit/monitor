"""
processing_service.py - Shared transactional processing for live and replay paths.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiosqlite

import persistence
from models import NormalizedEvent, RawEvent, Source
from normalization import normalize_serial, normalize_telegram
from parser_engine import parse_telegram
from resolution_engine import resolve_slot
from schedule_engine import get_window_range
from serial_parser import parse_serial_payload
from station_manager import StationManager
from station_resolver import StationResolver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProcessResult:
    inserted: bool
    raw_event_id: Optional[int]
    status: Optional[str]
    normalized_count: int
    deduped: bool = False


class ProcessingService:
    """Owns the writer connection and processes raw events transactionally."""

    def __init__(
        self,
        station_manager: StationManager,
        db_path: Path | None = None,
        *,
        max_retries: int = 5,
        base_retry_delay: float = 0.1,
    ) -> None:
        self._station_mgr = station_manager
        self._resolver = StationResolver(station_manager)
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None
        self._max_retries = max_retries
        self._base_retry_delay = base_retry_delay

    async def start(self) -> None:
        if self._db is None:
            self._db = await persistence.create_db_connection(self._db_path)

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def process_new_raw(self, raw: RawEvent) -> ProcessResult:
        try:
            return await self._with_retry(self._process_new_raw_once, raw)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raw_event_id = await persistence.record_processing_failure(
                raw,
                exception=exc,
                db_path=self._db_path,
            )
            logger.error(
                "Processing failed for source=%s source_event_id=%s; preserved raw event id=%s",
                raw.source.value,
                raw.source_event_id,
                raw_event_id,
                exc_info=True,
            )
            return ProcessResult(
                inserted=True,
                raw_event_id=raw_event_id,
                status="error",
                normalized_count=0,
            )

    async def reprocess_existing_raw(
        self,
        raw_event_id: int,
        raw: RawEvent,
        *,
        replace: bool = False,
    ) -> ProcessResult:
        try:
            return await self._with_retry(
                self._reprocess_existing_raw_once,
                raw_event_id,
                raw,
                replace=replace,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            preserved_raw_event_id = await persistence.record_processing_failure(
                raw,
                raw_event_id=raw_event_id,
                exception=exc,
                db_path=self._db_path,
            )
            logger.error(
                "Reprocessing failed for raw_event_id=%s source=%s; preserved raw event id=%s",
                raw_event_id,
                raw.source.value,
                preserved_raw_event_id,
                exc_info=True,
            )
            return ProcessResult(
                inserted=True,
                raw_event_id=preserved_raw_event_id,
                status="error",
                normalized_count=0,
            )

    def _require_db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("ProcessingService.start() must be awaited before use")
        return self._db

    async def _with_retry(self, func, *args, **kwargs):
        db = self._require_db()
        for attempt in range(self._max_retries):
            try:
                return await func(*args, **kwargs)
            except aiosqlite.OperationalError as exc:
                if "locked" not in str(exc).lower() or attempt >= self._max_retries - 1:
                    raise
                await db.rollback()
                delay = self._base_retry_delay * (2 ** attempt)
                logger.warning(
                    "DB locked during transactional processing (attempt %d/%d), retrying in %.2fs",
                    attempt + 1,
                    self._max_retries,
                    delay,
                )
                await asyncio.sleep(delay)

    async def _process_new_raw_once(self, raw: RawEvent) -> ProcessResult:
        db = self._require_db()
        await db.execute("BEGIN IMMEDIATE")
        try:
            inserted, raw_event_id = await persistence.insert_raw_event(raw, db=db)
            if not inserted:
                await db.rollback()
                return ProcessResult(
                    inserted=False,
                    raw_event_id=None,
                    status=None,
                    normalized_count=0,
                    deduped=True,
                )

            result = await self._process_existing_raw_in_tx(
                raw_event_id=raw_event_id,
                raw=raw,
                replace=False,
            )
            await db.commit()
            return result
        except BaseException:
            await db.rollback()
            raise

    async def _reprocess_existing_raw_once(
        self,
        raw_event_id: int,
        raw: RawEvent,
        *,
        replace: bool,
    ) -> ProcessResult:
        db = self._require_db()
        await db.execute("BEGIN IMMEDIATE")
        try:
            result = await self._process_existing_raw_in_tx(
                raw_event_id=raw_event_id,
                raw=raw,
                replace=replace,
            )
            await db.commit()
            return result
        except BaseException:
            await db.rollback()
            raise

    async def _process_existing_raw_in_tx(
        self,
        *,
        raw_event_id: int,
        raw: RawEvent,
        replace: bool,
    ) -> ProcessResult:
        db = self._require_db()
        affected_slots: set[tuple[int, str, str]] = set()
        if replace:
            affected_slots |= await persistence.get_slots_for_raw(raw_event_id, db=db)
            await persistence.delete_normalized_events_for_raw(raw_event_id, db=db)

        normalized, status, parse_error = await self._normalize_raw(
            raw_event_id=raw_event_id,
            raw=raw,
            db=db,
        )

        if not normalized:
            await persistence.update_raw_event_status(
                raw_event_id,
                status,
                parse_error,
                db=db,
            )
            await self._resolve_slots(affected_slots, db)
            return ProcessResult(
                inserted=True,
                raw_event_id=raw_event_id,
                status=status,
                normalized_count=0,
            )

        for norm in normalized:
            await persistence.insert_normalized_event(norm, db=db)
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
                    db=db,
                )

        affected_slots |= await persistence.get_slots_for_raw(raw_event_id, db=db)
        await persistence.update_raw_event_status(
            raw_event_id,
            status,
            parse_error,
            db=db,
        )
        await self._resolve_slots(affected_slots, db)

        return ProcessResult(
            inserted=True,
            raw_event_id=raw_event_id,
            status=status,
            normalized_count=len(normalized),
        )

    async def _normalize_raw(
        self,
        *,
        raw_event_id: int,
        raw: RawEvent,
        db: aiosqlite.Connection,
    ) -> tuple[list[NormalizedEvent], str, Optional[str]]:
        meta = raw.transport_meta or {}
        parse_error: Optional[str] = None
        normalized: list[NormalizedEvent] = []

        if raw.source == Source.TELEGRAM:
            parsed = parse_telegram(raw.raw_payload)
            if not parsed:
                return [], "error", "telegram_parse_failed"

            norm = normalize_telegram(
                parsed,
                raw_event_id=raw_event_id,
                station_manager=self._station_mgr,
                resolver=self._resolver,
            )
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
                        norm.station_id,
                        start,
                        end,
                        db=db,
                    ):
                        norm.tone = False
                        norm.is_valid_report = False
            normalized = [norm]

        elif raw.source == Source.SERIAL:
            parsed_list = parse_serial_payload(raw.raw_payload)
            if not parsed_list:
                status = "partial" if meta.get("frame_overflow") else "error"
                error = "serial_frame_overflow" if meta.get("frame_overflow") else "serial_parse_failed"
                return [], status, error

            invalid_header_error: Optional[str] = None
            for parsed in parsed_list:
                try:
                    norm = normalize_serial(
                        parsed,
                        raw_event_id=raw_event_id,
                        resolver=self._resolver,
                        received_at=raw.received_at,
                    )
                except ValueError as exc:
                    if invalid_header_error is None:
                        invalid_header_error = str(exc)
                    continue
                normalized.append(norm)

            if not normalized:
                status = "partial" if meta.get("frame_overflow") else "error"
                return [], status, invalid_header_error or "serial_parse_failed"

            if meta.get("frame_overflow"):
                parse_error = "serial_frame_overflow"
                return normalized, "partial", parse_error
            if invalid_header_error:
                return normalized, "partial", invalid_header_error

        else:
            return [], "error", f"unknown_source:{raw.source}"

        return normalized, "complete", parse_error

    async def _resolve_slots(
        self,
        slots: set[tuple[int, str, str]],
        db: aiosqlite.Connection,
    ) -> None:
        for station_id, report_date, report_slot in sorted(slots):
            await resolve_slot(
                station_id,
                report_date,
                report_slot,
                db=db,
            )
