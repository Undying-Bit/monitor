"""
config.py - Central configuration and environment loading.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Application metadata
APP_VERSION = "0.3.01"


# Directories
BASE_DIR = Path(__file__).parent
DATABASE_DIR = BASE_DIR / "databases"
LOG_DIR = BASE_DIR / "logs"

DATABASE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)


# Environment
ENV_FILE = BASE_DIR / ".env"
load_dotenv(ENV_FILE)

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
SESSION_NAME = os.getenv("SESSION_NAME", "monitor_session")


# Database paths
STATIONS_DB = DATABASE_DIR / "estaciones.db"
PARSED_DB = DATABASE_DIR / "mensajes.db"


# Timezone
LOCAL_TIMEZONE = os.getenv("LOCAL_TIMEZONE", "America/Mexico_City")


# Telegram
TELEGRAM_CATCHUP_OVERLAP_MESSAGES = int(
    os.getenv("TELEGRAM_CATCHUP_OVERLAP_MESSAGES", "20")
)
TELEGRAM_TIMEOUT_SECONDS = int(os.getenv("TELEGRAM_TIMEOUT_SECONDS", "20"))
TELEGRAM_REQUEST_RETRIES = int(os.getenv("TELEGRAM_REQUEST_RETRIES", "10"))
TELEGRAM_CONNECTION_RETRIES = int(os.getenv("TELEGRAM_CONNECTION_RETRIES", "-1"))
TELEGRAM_RETRY_DELAY_SECONDS = int(os.getenv("TELEGRAM_RETRY_DELAY_SECONDS", "5"))


# Serial
SERIAL_PORTS = os.getenv("SERIAL_PORTS", "COM3")
SERIAL_BAUDRATE = int(os.getenv("SERIAL_BAUDRATE", "9600"))
SERIAL_BYTESIZE = int(os.getenv("SERIAL_BYTESIZE", "8"))
SERIAL_PARITY = os.getenv("SERIAL_PARITY", "N")
SERIAL_STOPBITS = int(os.getenv("SERIAL_STOPBITS", "1"))
SERIAL_TIMEOUT_SECONDS = float(os.getenv("SERIAL_TIMEOUT_SECONDS", "1.0"))
SERIAL_RECONNECT_DELAY_SECONDS = float(
    os.getenv("SERIAL_RECONNECT_DELAY_SECONDS", "5.0")
)
SERIAL_MAX_FRAME_BYTES = int(os.getenv("SERIAL_MAX_FRAME_BYTES", "4096"))


# Parsing
PHONE_LENGTH = 10
DEFAULT_COUNTRY_CODE = "52"
CONFIDENCE_RESOLVE_THRESHOLD = int(
    os.getenv("CONFIDENCE_RESOLVE_THRESHOLD", "70")
)
STATE_PAIR_WINDOW_SECONDS = int(
    os.getenv("STATE_PAIR_WINDOW_SECONDS", "100")
)


# Schedule / tono
REPORT_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]
REPORT_MINUTE = 45  # Main windows at HH:45
TONO_WINDOW_SECONDS = 120  # Forward-only window: HH:45:00 through HH:47:00


# Logging
APP_LOG = LOG_DIR / "app.log"
PARSING_ERRORS_LOG = LOG_DIR / "parsing_errors.log"
SYSTEM_HEALTH_LOG = LOG_DIR / "system_health.log"
