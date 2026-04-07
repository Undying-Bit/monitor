"""
serial_main.py - Entry point for the Serial Monitor service.
"""
from __future__ import annotations

import asyncio
import logging
import sys

from config import SERIAL_PORTS
from main import _setup_logging


async def main() -> None:
    _setup_logging()
    logger = logging.getLogger("serial_main")

    from station_manager import StationManager
    from orchestrator import Orchestrator
    from serial_client import SerialIngress
    import persistence

    await persistence.init_db()
    logger.info("Database ready")

    sm = StationManager()
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    orch = Orchestrator(queue, sm)
    consumer_task = asyncio.create_task(orch.run())

    ports = [p.strip() for p in SERIAL_PORTS.split(",") if p.strip()]
    if not ports:
        logger.error("No serial ports configured (SERIAL_PORTS is empty)")
        return
    ingress = SerialIngress(queue, ports)
    await ingress.start()

    logger.info("Serial monitor running")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        await ingress.stop()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        logger.info("Serial monitor stopped")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
