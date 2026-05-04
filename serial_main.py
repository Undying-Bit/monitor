"""
serial_main.py - Entry point for the Serial Monitor service.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
from collections.abc import Callable

from config import SERIAL_PORTS
from main import _setup_logging


def _install_shutdown_handlers(
    shutdown_event: asyncio.Event,
    logger: logging.Logger,
) -> Callable[[], None]:
    """Install process signal handlers that request an async shutdown."""
    loop = asyncio.get_running_loop()
    previous_handlers: dict[
        signal.Signals,
        signal.Handlers | int | Callable[..., object] | None,
    ] = {}
    loop_signals: list[signal.Signals] = []
    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signals.append(signal.SIGBREAK)

    def request_shutdown(sig: signal.Signals) -> None:
        if shutdown_event.is_set():
            return
        logger.info("Shutdown requested by %s", sig.name)
        shutdown_event.set()

    for sig in signals:
        try:
            loop.add_signal_handler(sig, request_shutdown, sig)
        except (NotImplementedError, RuntimeError):
            previous_handlers[sig] = signal.getsignal(sig)

            def handler(
                _signum: int,
                _frame: object,
                sig: signal.Signals = sig,
            ) -> None:
                loop.call_soon_threadsafe(request_shutdown, sig)

            signal.signal(sig, handler)
        else:
            loop_signals.append(sig)

    def cleanup() -> None:
        for sig in loop_signals:
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.remove_signal_handler(sig)
        for sig, previous_handler in previous_handlers.items():
            if previous_handler is None:
                continue
            signal.signal(sig, previous_handler)

    return cleanup


async def main() -> None:
    _setup_logging()
    logger = logging.getLogger("serial_main")

    from station_manager import StationManager
    from orchestrator import Orchestrator
    from serial_client import SerialIngress
    import persistence

    ports = [p.strip() for p in SERIAL_PORTS.split(",") if p.strip()]
    if not ports:
        logger.error("No serial ports configured (SERIAL_PORTS is empty)")
        return

    await persistence.init_db()
    logger.info("Database ready")

    sm = StationManager()
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)
    orch = Orchestrator(queue, sm)
    consumer_task = asyncio.create_task(orch.run())

    ingress = SerialIngress(queue, ports)
    await ingress.start()

    shutdown_event = asyncio.Event()
    cleanup_signal_handlers = _install_shutdown_handlers(shutdown_event, logger)

    logger.info("Serial monitor running")
    try:
        await shutdown_event.wait()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        cleanup_signal_handlers()
        ingress.request_stop()
        await ingress.wait_stopped()
        await queue.join()
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
