import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator import Orchestrator
from models import NormalizedEvent, RawEvent, Source


@pytest.mark.asyncio
async def test_orchestrator_keeps_state_rows_valid():
    queue = asyncio.Queue()
    sm = MagicMock()

    raw = RawEvent(
        source=Source.TELEGRAM,
        source_event_id="123",
        raw_payload="+525512345678 18/03/2026 05:45:50 SOME_CONTENT",
        received_at=datetime.utcnow(),
        transport_meta={},
    )

    norm = NormalizedEvent(
        raw_event_id=1,
        source=Source.TELEGRAM,
        station_id=1,
        station_name="TEST",
        station_code=None,
        event_type="OPEN",
        event_class="STATE",
        report_date="2026-03-18",
        report_slot="05:45",
        event_time_local="2026-03-18 05:45:50",
        event_time_utc="2026-03-18T11:45:50+00:00",
        tone=True,
        priority=100,
        is_valid_report=True,
        parser_version="telegram:v1.1",
        confidence_score=80,
        payload_json={},
    )

    orch = Orchestrator(queue, sm)

    with patch("persistence.insert_raw_event", new_callable=AsyncMock) as mock_ins_raw, \
         patch("persistence.update_raw_event_status", new_callable=AsyncMock) as mock_status, \
         patch("persistence.insert_normalized_event", new_callable=AsyncMock) as mock_ins_norm, \
         patch("persistence.has_open_and_close", new_callable=AsyncMock) as mock_has_oc, \
         patch("resolution_engine.resolve_slot", new_callable=AsyncMock), \
         patch("orchestrator.parse_telegram") as mock_parse, \
         patch("orchestrator.normalize_telegram") as mock_norm:

        mock_ins_raw.return_value = (True, 1)
        mock_has_oc.return_value = True
        mock_parse.return_value = MagicMock(timestamp=datetime(2026, 3, 18, 5, 45, 50))
        mock_norm.return_value = norm
        mock_ins_norm.return_value = 10

        await orch._process(raw)

        assert norm.is_valid_report is True
        assert norm.tone is True
        mock_has_oc.assert_not_awaited()
        mock_status.assert_called_with(1, "parsed", None)
