import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from models import RawEvent, Source
from orchestrator import Orchestrator
from processing_service import ProcessResult


@pytest.mark.asyncio
async def test_orchestrator_delegates_processing_to_shared_service():
    queue = asyncio.Queue()
    station_manager = MagicMock()
    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="123",
        raw_payload="+525512345678 18/03/2026 05:45:50 SOME_CONTENT",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    orch = Orchestrator(queue, station_manager)
    orch._processor.process_new_raw = AsyncMock(
        return_value=ProcessResult(
            inserted=True,
            raw_event_id=1,
            status="complete",
            normalized_count=1,
        )
    )

    await orch._process(raw)

    orch._processor.process_new_raw.assert_awaited_once_with(raw)
