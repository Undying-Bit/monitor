import pytest
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from orchestrator import Orchestrator
from models import RawMessage, ParsedMessage, MessageType

@pytest.mark.asyncio
async def test_orchestrator_tono_suppression():
    # Setup mocks
    queue = asyncio.Queue()
    sm = MagicMock()
    sm.get_red.return_value = "RED_A"
    
    # Mock persistence functions
    with patch("persistence.message_exists", new_callable=AsyncMock) as mock_exists, \
         patch("persistence.insert_message", new_callable=AsyncMock) as mock_insert, \
         patch("persistence.has_open_and_close", new_callable=AsyncMock) as mock_has_oc:
        
        mock_exists.return_value = False
        mock_insert.return_value = True
        
        # Scenario: Window has OPEN and CLOSE already
        mock_has_oc.return_value = True
        
        orch = Orchestrator(queue, sm)
        
        # Create a raw message that would be TONO
        # 05:45:50 is usually TONO
        raw = RawMessage(
            telegram_id=123,
            raw_text="+525512345678 18/03/2026 05:45:50 SOME_CONTENT",
            receive_timestamp=datetime.now()
        )
        
        # Mock the parser to return a ParsedMessage
        with patch("orchestrator.parse") as mock_parse:
            parsed = ParsedMessage(
                telegram_id=123,
                telefono="5512345678",
                estacion="TEST_STATION",
                tipo_mensaje=MessageType.SINGLE,
                texto="SOME_CONTENT",
                timestamp=datetime(2026, 3, 18, 5, 45, 50)
            )
            mock_parse.return_value = parsed
            
            # Run one process cycle
            await orch._process(raw)
            
            # Assertions
            mock_has_oc.assert_called_once()
            # It should have been marked tono=False because mock_has_oc returned True
            assert parsed.tono is False

@pytest.mark.asyncio
async def test_orchestrator_tono_allowed():
    # Setup mocks
    queue = asyncio.Queue()
    sm = MagicMock()
    sm.get_red.return_value = "RED_A"
    
    with patch("persistence.message_exists", new_callable=AsyncMock) as mock_exists, \
         patch("persistence.insert_message", new_callable=AsyncMock) as mock_insert, \
         patch("persistence.has_open_and_close", new_callable=AsyncMock) as mock_has_oc:
        
        mock_exists.return_value = False
        mock_insert.return_value = True
        
        # Scenario: Window does NOT have both OPEN and CLOSE yet
        mock_has_oc.return_value = False
        
        orch = Orchestrator(queue, sm)
        
        raw = RawMessage(
            telegram_id=124,
            raw_text="+525512345678 18/03/2026 05:45:50 SOME_CONTENT",
            receive_timestamp=datetime.now()
        )
        
        with patch("orchestrator.parse") as mock_parse:
            parsed = ParsedMessage(
                telegram_id=124,
                telefono="5512345678",
                estacion="TEST_STATION",
                tipo_mensaje=MessageType.SINGLE,
                texto="SOME_CONTENT",
                timestamp=datetime(2026, 3, 18, 5, 45, 50)
            )
            mock_parse.return_value = parsed
            
            await orch._process(raw)
            
            # It should remain tono=True because mock_has_oc returned False
            # (Assuming 05:45:50 is within TONO_WINDOW_SECONDS)
            assert parsed.tono is True


@pytest.mark.asyncio
async def test_orchestrator_skips_unregistered_phone():
    queue = asyncio.Queue()
    sm = MagicMock()
    sm.lookup_by_phone.return_value = []

    with patch("persistence.message_exists", new_callable=AsyncMock) as mock_exists, \
         patch("persistence.insert_message", new_callable=AsyncMock) as mock_insert, \
         patch("persistence.has_open_and_close", new_callable=AsyncMock) as mock_has_oc:

        mock_exists.return_value = False
        mock_insert.return_value = True
        mock_has_oc.return_value = False

        orch = Orchestrator(queue, sm)

        raw = RawMessage(
            telegram_id=125,
            raw_text="+525512345678 18/03/2026 05:45:50 SOME_CONTENT",
            receive_timestamp=datetime.now()
        )

        with patch("orchestrator.parse") as mock_parse:
            parsed = ParsedMessage(
                telegram_id=125,
                telefono="5512345678",
                estacion="Estacion 5512345678",
                tipo_mensaje=MessageType.SINGLE,
                texto="SOME_CONTENT",
                timestamp=datetime(2026, 3, 18, 5, 45, 50)
            )
            mock_parse.return_value = parsed

            await orch._process(raw)

            mock_insert.assert_not_called()
            mock_has_oc.assert_not_called()
