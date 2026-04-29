"""
main.py — Entry point for the Telegram Monitor service.

Wires all modules, sets up logging, and runs the asyncio event loop.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections.abc import Sequence

from config import APP_LOG, APP_VERSION, PARSING_ERRORS_LOG, SYSTEM_HEALTH_LOG


# ── Logging setup ────────────────────────────────────────────

def _setup_logging() -> None:
    """Configure the three-file logging strategy + console output."""
    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Root logger
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    # Remove existing handlers
    for h in root.handlers[:]:
        root.removeHandler(h)

    # Console
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)

    # app.log — INFO flow
    app_fh = logging.FileHandler(str(APP_LOG), encoding="utf-8")
    app_fh.setLevel(logging.INFO)
    app_fh.setFormatter(fmt)
    root.addHandler(app_fh)

    # system_health.log — WARNING+
    health_fh = logging.FileHandler(str(SYSTEM_HEALTH_LOG), encoding="utf-8")
    health_fh.setLevel(logging.WARNING)
    health_fh.setFormatter(fmt)
    root.addHandler(health_fh)

    # parsing_errors.log is set up lazily inside parser_engine.py

    # Suppress noisy libraries
    for lib in ("telethon", "aiosqlite"):
        logging.getLogger(lib).setLevel(logging.WARNING)


# ── Banner ───────────────────────────────────────────────────

BANNER = r"""
╔══════════════════════════════════════════════╗
║      SERVICIO DE MONITOREO TELEGRAM          ║
║                                              ║
╚══════════════════════════════════════════════╝
"""


# ── Main ─────────────────────────────────────────────────────

def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Telegram monitor service.")
    parser.add_argument(
        "--telegram-catchup-overlap-messages",
        type=_positive_int,
        help=(
            "Expand the Telegram startup catch-up window to at least this many "
            "messages behind the stored watermark."
        ),
    )
    parser.add_argument(
        "--telegram-catchup-last-days",
        type=_positive_int,
        help=(
            "Ignore the stored Telegram watermark for this launch and replay "
            "messages from the last N days."
        ),
    )
    return parser.parse_args(argv)


async def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    _setup_logging()
    logger = logging.getLogger("main")

    print(BANNER)
    logger.info("Initializing Telegram Monitor v%s...", APP_VERSION)

    # Late imports so logging is configured first
    from station_manager import StationManager
    from orchestrator import Orchestrator
    from telegram_client import TelegramIngress
    import persistence

    # 1. Database
    await persistence.init_db()
    logger.info("Database ready")

    # 2. Station manager
    sm = StationManager()

    # 3. Shared queue (buffering layer)
    queue: asyncio.Queue = asyncio.Queue(maxsize=500)

    # 4. Orchestrator (consumer)
    orch = Orchestrator(queue, sm)
    consumer_task = asyncio.create_task(orch.run())

    # 5. Telegram ingress (producer)
    ingress = TelegramIngress(
        queue,
        catchup_overlap_messages=args.telegram_catchup_overlap_messages,
        catchup_last_days=args.telegram_catchup_last_days,
    )
    await ingress.start()

    logger.info("All modules initialised — entering event loop")

    try:
        await ingress.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user")
    finally:
        await ingress.stop()
        await queue.join()
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        logger.info("Telegram Monitor stopped")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
