"""
reprocess_raw_events.py - Re-run transactional parsing/normalization for raw_events.

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
from typing import Optional

from models import RawEvent, Source
from persistence import open_db, retry_on_locked
from processing_service import ProcessingService
from station_manager import StationManager

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


def _make_raw_event(
    row: tuple,
) -> tuple[int, RawEvent]:
    raw_event_id, source, source_event_id, received_at, raw_payload, meta_json = row
    meta: dict = {}
    if meta_json:
        try:
            loaded = json.loads(meta_json)
        except (TypeError, json.JSONDecodeError) as exc:
            logger.warning(
                "Corrupted transport_meta_json for raw_event_id=%s (%s); using empty metadata",
                raw_event_id,
                exc,
            )
        else:
            if isinstance(loaded, dict):
                meta = loaded
            else:
                logger.warning(
                    "Non-object transport_meta_json for raw_event_id=%s; using empty metadata",
                    raw_event_id,
                )

    raw = RawEvent(
        source=Source(source),
        source_event_id=source_event_id,
        raw_payload=raw_payload,
        received_at=datetime.fromisoformat(received_at),
        transport_meta=meta,
    )
    return raw_event_id, raw


async def main() -> None:
    parser = argparse.ArgumentParser(description="Reprocess raw_events with updated parser.")
    parser.add_argument("--source", default="all", choices=["all", "telegram", "serial"])
    parser.add_argument("--since", help="Start received_at (YYYY-MM-DD or ISO timestamp).")
    parser.add_argument("--until", help="End received_at (YYYY-MM-DD or ISO timestamp).")
    parser.add_argument("--status", choices=["pending", "complete", "partial", "error"])
    parser.add_argument("--limit", type=int, help="Max rows to process.")
    parser.add_argument("--offset", type=int, help="Row offset.")
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing normalized rows for each raw_event_id before rebuilding.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Scan only, do not write changes.")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    import persistence

    await persistence.init_db()
    station_manager = StationManager()
    processor = ProcessingService(station_manager)
    await processor.start()

    try:
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

        total_complete = 0
        total_partial = 0
        total_error = 0
        total_normalized = 0

        for row in rows:
            raw_event_id, raw = _make_raw_event(row)
            logger.info(
                "Reprocessing raw_event_id=%s source=%s",
                raw_event_id,
                raw.source.value,
            )
            result = await processor.reprocess_existing_raw(
                raw_event_id,
                raw,
                replace=args.replace,
            )
            total_normalized += result.normalized_count
            if result.status == "complete":
                total_complete += 1
            elif result.status == "partial":
                total_partial += 1
            else:
                total_error += 1

        logger.info(
            "Reprocess complete: raw_events=%d complete=%d partial=%d error=%d normalized=%d",
            len(rows),
            total_complete,
            total_partial,
            total_error,
            total_normalized,
        )
    finally:
        await processor.close()


if __name__ == "__main__":
    asyncio.run(main())
