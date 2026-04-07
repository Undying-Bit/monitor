"""
serial_client.py - Serial ingress service using pyserial.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from datetime import datetime

import serial

from config import (
    SERIAL_BAUDRATE,
    SERIAL_BYTESIZE,
    SERIAL_MAX_FRAME_BYTES,
    SERIAL_PARITY,
    SERIAL_RECONNECT_DELAY_SECONDS,
    SERIAL_STOPBITS,
    SERIAL_TIMEOUT_SECONDS,
)
from models import RawEvent, Source

logger = logging.getLogger(__name__)


class SerialIngress:
    """Reads from one or more COM ports and feeds RawEvent into a queue."""

    def __init__(self, queue: asyncio.Queue[RawEvent], ports: list[str]) -> None:
        self._queue = queue
        self._ports = ports
        self._stop_event = threading.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for port in self._ports:
            task = asyncio.create_task(asyncio.to_thread(self._run_port, port, loop))
            self._tasks.append(task)
        logger.info("Serial ingress started for ports: %s", ", ".join(self._ports))

    def _run_port(self, port: str, loop: asyncio.AbstractEventLoop) -> None:
        seq = 0
        buffer = bytearray()
        parity_map = {
            "N": serial.PARITY_NONE,
            "E": serial.PARITY_EVEN,
            "O": serial.PARITY_ODD,
            "M": serial.PARITY_MARK,
            "S": serial.PARITY_SPACE,
        }
        parity = parity_map.get(SERIAL_PARITY.upper(), serial.PARITY_NONE)
        while not self._stop_event.is_set():
            try:
                with serial.Serial(
                    port=port,
                    baudrate=SERIAL_BAUDRATE,
                    bytesize=SERIAL_BYTESIZE,
                    parity=parity,
                    stopbits=SERIAL_STOPBITS,
                    timeout=SERIAL_TIMEOUT_SECONDS,
                ) as ser:
                    logger.info("Serial connected: %s", port)
                    while not self._stop_event.is_set():
                        chunk = ser.read(ser.in_waiting or 1)
                        if not chunk:
                            continue
                        buffer.extend(chunk)
                        while True:
                            term_index = buffer.find(b"NNNN")
                            if term_index != -1:
                                frame = buffer[: term_index + 4]
                                buffer = buffer[term_index + 4 :]
                                payload = frame.decode(errors="replace")
                                seq += 1
                                received_at = datetime.utcnow()
                                raw = RawEvent(
                                    source=Source.SERIAL,
                                    source_event_id=(
                                        f"{port}:{received_at.isoformat()}:{seq}"
                                    ),
                                    raw_payload=payload,
                                    received_at=received_at,
                                    transport_meta={
                                        "port": port,
                                        "baudrate": SERIAL_BAUDRATE,
                                        "bytesize": SERIAL_BYTESIZE,
                                        "parity": SERIAL_PARITY,
                                        "stopbits": SERIAL_STOPBITS,
                                        "terminator_found": True,
                                    },
                                )
                                def _enqueue(item: RawEvent) -> None:
                                    try:
                                        self._queue.put_nowait(item)
                                    except asyncio.QueueFull:
                                        logger.warning(
                                            "Queue full, dropping serial payload from %s", port
                                        )
                                loop.call_soon_threadsafe(_enqueue, raw)
                                continue
                            if len(buffer) >= SERIAL_MAX_FRAME_BYTES:
                                payload = buffer.decode(errors="replace")
                                buffer.clear()
                                # Reset buffer and resume fresh to avoid carryover.
                                seq += 1
                                logger.warning(
                                    "Serial frame overflow on %s (%d bytes)",
                                    port,
                                    SERIAL_MAX_FRAME_BYTES,
                                )
                                received_at = datetime.utcnow()
                                raw = RawEvent(
                                    source=Source.SERIAL,
                                    source_event_id=(
                                        f"{port}:{received_at.isoformat()}:{seq}"
                                    ),
                                    raw_payload=payload,
                                    received_at=received_at,
                                    transport_meta={
                                        "port": port,
                                        "baudrate": SERIAL_BAUDRATE,
                                        "bytesize": SERIAL_BYTESIZE,
                                        "parity": SERIAL_PARITY,
                                        "stopbits": SERIAL_STOPBITS,
                                        "terminator_found": False,
                                        "frame_overflow": True,
                                    },
                                )
                                def _enqueue(item: RawEvent) -> None:
                                    try:
                                        self._queue.put_nowait(item)
                                    except asyncio.QueueFull:
                                        logger.warning(
                                            "Queue full, dropping serial payload from %s", port
                                        )
                                loop.call_soon_threadsafe(_enqueue, raw)
                                continue
                            break
            except serial.SerialException as exc:
                logger.warning("Serial error on %s: %s", port, exc)
                time.sleep(SERIAL_RECONNECT_DELAY_SECONDS)

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Serial ingress stopped")
