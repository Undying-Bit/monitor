import pytest
import asyncio
import os
from datetime import datetime, timedelta
import aiosqlite
from persistence import init_db, insert_message, has_open_and_close
from models import ParsedMessage, MessageType
from config import PARSED_DB

@pytest.mark.asyncio
async def test_has_open_and_close():
    # Use the test database from config (it should be fine if we clean up)
    # Actually, let's just use it and rely on its existence
    
    # Ensure DB is initialized
    await init_db()
    
    station = "TEST_STATION"
    start = datetime(2026, 3, 18, 5, 45, 0)
    end = start + timedelta(seconds=120)
    
    # 1. No messages
    assert await has_open_and_close(station, start, end) is False
    
    # 2. Only OPEN
    msg_open = ParsedMessage(
        telegram_id=999901,
        telefono="1234567890",
        estacion=station,
        tipo_mensaje=MessageType.OPEN,
        texto="OPEN",
        timestamp=start + timedelta(seconds=10),
        tono=True
    )
    await insert_message(msg_open)
    assert await has_open_and_close(station, start, end) is False
    
    # 3. OPEN and CLOSE
    msg_close = ParsedMessage(
        telegram_id=999902,
        telefono="1234567890",
        estacion=station,
        tipo_mensaje=MessageType.CLOSE,
        texto="CLOSE",
        timestamp=start + timedelta(seconds=20),
        tono=True
    )
    await insert_message(msg_close)
    assert await has_open_and_close(station, start, end) is True

    # Cleanup (optional but good for repeatability if using same file)
    async with aiosqlite.connect(str(PARSED_DB)) as db:
        await db.execute("DELETE FROM mensajes WHERE estacion = 'TEST_STATION'")
        await db.commit()
