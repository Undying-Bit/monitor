"""
reprocess_raw_events.py - Re-run parsing/normalization for raw_events.

Usage examples:
  python reprocess_raw_events.py --source telegram --since 2026-03-01 --replace
  python reprocess_raw_events.py --source serial --status error --limit 500
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime
from typing import Iterable, Optional, Tuple

from models import RawEvent, Source, NormalizedEvent
from normalization import normalize_serial, normalize_telegram
from parser_engine import parse_telegram
from resolution_engine import resolve_slot
from schedule_engine import get_window_range
from serial_parser import parse_serial_payload
from station_manager import StationManager
from station_resolver import StationResolver
import persistence
from persistence import open_db, retry_on_locked

logger = logging.getLogger(__name__)


def _parse_datetime_arg(value: str, end_of_day: bool = False) -> str:
    value = value.strip()
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        dt = datetime.strptime(value, "%Y-%m-%d")
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59)
        return dt.isoformat()
    return datetime.fromisoformat(value).isoformat()


@retry_on_locked()
async def _fetch_raw_events(
    source: Optional[str],
    since: Optional[str],
    until: Optional[str],
    status: Optional[str],
    limit: Optional[int],
    offset: Optional[int],
) -> list[tuple]:
    query = (
        "SELECT id, source, source_event_id, received_at, raw_payload, transport_meta_json "
        "FROM raw_events WHERE 1=1"
    )
    params: list = []
    if source and source != "all":
        query += " AND source = ?"
        params.append(source)
    if since:
        query += " AND received_at >= ?"
        params.append(since)
    if until:
        query += " AND received_at <= ?"
        params.append(until)
    if status:
        query += " AND parse_status = ?"
        params.append(status)
    query += " ORDER BY id ASC"
    if limit is not None:
        query += " LIMIT ?"
        params.append(limit)
        if offset:
            query += " OFFSET ?"
            params.append(offset)
    elif offset:
        query += " LIMIT -1 OFFSET ?"
        params.append(offset)

    async with open_db() as db:
        async with db.execute(query, tuple(params)) as cursor:
            return await cursor.fetchall()


@retry_on_locked()
async def _fetch_slots_for_raw(raw_event_id: int) -> set[Tuple[int, str, str]]:
    async with open_db() as db:
        async with db.execute(
            """
            SELECT station_id, report_date, report_slot
            FROM normalized_events
            WHERE raw_event_id = ?
            """,
            (raw_event_id,),
        ) as cursor:
            rows = await cursor.fetchall()
    slots: set[Tuple[int, str, str]] = set()
    for station_id, report_date, report_slot in rows:
        if station_id and report_date and report_slot:
            slots.add((station_id, report_date, report_slot))
    return slots


@retry_on_locked()
async def _delete_normalized_for_raw(raw_event_id: int) -> None:
    await persistence.delete_normalized_events_for_raw(raw_event_id)


def _make_raw_event(
    row: tuple,
) -> tuple[int, RawEvent, dict]:
    raw_event_id, source, source_event_id, received_at, raw_payload, meta_json = row
    meta = json.loads(meta_json) if meta_json else {}
    raw = RawEvent(
        source=Source(source),
        source_event_id=source_event_id,
        raw_payload=raw_payload,
        received_at=datetime.fromisoformat(received_at),
        transport_meta=meta,
    )
    return raw_event_id, raw, meta


async def _reprocess_one(
    raw_event_id: int,
    raw: RawEvent,
    meta: dict,
    station_manager: StationManager,
    resolver: StationResolver,
    replace: bool,
) -> tuple[int, int, set[Tuple[int, str, str]]]:
    parse_error: Optional[str] = None
    normalized: list[NormalizedEvent] = []

    if raw.source == Source.TELEGRAM:
        parsed = parse_telegram(raw.raw_payload)
        if not parsed:
            parse_error = "telegram_parse_failed"
        else:
            norm = normalize_telegram(
                parsed,
                raw_event_id=raw_event_id,
                station_manager=station_manager,
                resolver=resolver,
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
            for parsed in parsed_list:
                norm = normalize_serial(
                    parsed,
                    raw_event_id=raw_event_id,
                    resolver=resolver,
                    received_at=raw.received_at,
                )
                normalized.append(norm)

    else:
        parse_error = f"unknown_source:{raw.source}"

    affected_slots: set[Tuple[int, str, str]] = set()
    if replace:
        affected_slots |= await _fetch_slots_for_raw(raw_event_id)
        await _delete_normalized_for_raw(raw_event_id)

    if not normalized:
        status = "error"
        if raw.source == Source.SERIAL and meta.get("frame_overflow"):
            status = "partial"
            parse_error = "serial_frame_overflow"
        await persistence.update_raw_event_status(raw_event_id, status, parse_error)
        return 0, 0, affected_slots

    if raw.source == Source.SERIAL and meta.get("frame_overflow"):
        await persistence.update_raw_event_status(
            raw_event_id, "partial", "serial_frame_overflow"
        )
    else:
        await persistence.update_raw_event_status(raw_event_id, "parsed", None)

    inserted = 0
    for norm in normalized:
        norm_id = await persistence.insert_normalized_event(norm)
        inserted += 1
        logger.info(
            "Normalized event id=%s raw_event_id=%s type=%s slot=%s valid=%s",
            norm_id,
            raw_event_id,
            norm.event_type,
            norm.report_slot,
            norm.is_valid_report,
        )
        if norm.station_id and norm.report_date and norm.report_slot:
            if norm.event_type == "RWT" and norm.tone:
                await persistence.enforce_first_valid_for_slot(
                    norm.station_id,
                    norm.report_date,
                    norm.report_slot,
                    norm.event_type,
                )
            affected_slots.add((norm.station_id, norm.report_date, norm.report_slot))

    return len(normalized), inserted, affected_slots


async def _resolve_slots(slots: Iterable[Tuple[int, str, str]]) -> None:
    for station_id, report_date, report_slot in sorted(slots):
        await resolve_slot(station_id, report_date, report_slot)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess raw_events with updated parser.")
    parser.add_argument("--source", default="all", choices=["all", "telegram", "serial"])
    parser.add_argument("--since", help="Start received_at (YYYY-MM-DD or ISO timestamp).")
    parser.add_argument("--until", help="End received_at (YYYY-MM-DD or ISO timestamp).")
    parser.add_argument("--status", choices=["pending", "parsed", "partial", "error"])
    parser.add_argument("--limit", type=int, help="Max rows to process.")
    parser.add_argument("--offset", type=int, help="Row offset.")
    parser.add_argument("--replace", action="store_true", help="Delete existing normalized rows for each raw_event_id.")
    parser.add_argument("--dry-run", action="store_true", help="Scan only, do not write changes.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    await persistence.init_db()
    station_manager = StationManager()
    resolver = StationResolver(station_manager)

    since = _parse_datetime_arg(args.since) if args.since else None
    until = _parse_datetime_arg(args.until, end_of_day=True) if args.until else None

    rows = await _fetch_raw_events(
        args.source,
        since,
        until,
        args.status,
        args.limit,
        args.offset,
    )

    logger.info("Reprocess scan: %d raw_events selected", len(rows))
    if args.dry_run:
        return

    total_parsed = 0
    total_inserted = 0
    total_errors = 0

    for row in rows:
        raw_event_id, raw, meta = _make_raw_event(row)
        logger.info(
            "Reprocessing raw_event_id=%s source=%s",
            raw_event_id,
            raw.source.value,
        )
        parsed_count, inserted_count, slots = await _reprocess_one(
            raw_event_id,
            raw,
            meta,
            station_manager,
            resolver,
            replace=args.replace,
        )
        if parsed_count == 0:
            total_errors += 1
        total_parsed += parsed_count
        total_inserted += inserted_count
        if slots:
            await _resolve_slots(slots)

    logger.info(
        "Reprocess complete: raw_events=%d parsed=%d inserted=%d errors=%d",
        len(rows),
        total_parsed,
        total_inserted,
        total_errors,
    )


if __name__ == "__main__":
    asyncio.run(main())
