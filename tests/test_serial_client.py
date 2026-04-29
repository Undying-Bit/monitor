from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from models import RawEvent, Source
from serial_client import SerialIngress


@pytest.mark.asyncio
async def test_serial_enqueue_blocking_waits_for_capacity():
    queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=1)
    ingress = SerialIngress(queue, ["COM1"])
    loop = asyncio.get_running_loop()

    await queue.put(
        RawEvent(
            source=Source.SERIAL,
            source_event_id="existing",
            raw_payload="existing",
            received_at=datetime.utcnow(),
            transport_meta={},
        )
    )

    queued = RawEvent(
        source=Source.SERIAL,
        source_event_id="next",
        raw_payload="next",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    async def _free_slot() -> None:
        await asyncio.sleep(0.05)
        item = await queue.get()
        assert item.source_event_id == "existing"
        queue.task_done()

    release_task = asyncio.create_task(_free_slot())
    await asyncio.to_thread(ingress._enqueue_blocking, loop, queued, "COM1")
    await release_task

    item = await queue.get()
    assert item.source_event_id == "next"
    queue.task_done()


@pytest.mark.asyncio
async def test_serial_enqueue_blocking_exits_cleanly_when_shutdown_starts_with_full_queue():
    queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=1)
    ingress = SerialIngress(queue, ["COM1"])
    loop = asyncio.get_running_loop()

    await queue.put(
        RawEvent(
            source=Source.SERIAL,
            source_event_id="existing",
            raw_payload="existing",
            received_at=datetime.utcnow(),
            transport_meta={},
        )
    )

    queued = RawEvent(
        source=Source.SERIAL,
        source_event_id="next",
        raw_payload="next",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    enqueue_task = asyncio.create_task(
        asyncio.to_thread(ingress._enqueue_blocking, loop, queued, "COM1")
    )

    await asyncio.sleep(0.1)
    ingress.request_stop()
    await asyncio.wait_for(enqueue_task, timeout=2.0)

    item = await queue.get()
    assert item.source_event_id == "existing"
    queue.task_done()
    assert queue.empty()
