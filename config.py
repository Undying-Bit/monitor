"""
config.py — Central configuration and environment loading.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# ── Directories ──────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATABASE_DIR = BASE_DIR / "databases"
LOG_DIR = BASE_DIR / "logs"

DATABASE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# ── Environment ──────────────────────────────────────────────
ENV_FILE = BASE_DIR / ".env"
load_dotenv(ENV_FILE)

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
GROUP_ID = int(os.getenv("GROUP_ID", "0"))
SESSION_NAME = os.getenv("SESSION_NAME", "monitor_session")

# ── Database paths ───────────────────────────────────────────
STATIONS_DB = DATABASE_DIR / "estaciones.db"
PARSED_DB = DATABASE_DIR / "mensajes.db"

# ── Telegram ─────────────────────────────────────────────────
CATCHUP_LIMIT = 100  # messages to scan on restart

# ── Parsing ──────────────────────────────────────────────────
PHONE_LENGTH = 10
DEFAULT_COUNTRY_CODE = "52"

# ── Schedule / Tono ──────────────────────────────────────────
REPORT_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]
REPORT_MINUTE = 45  # Main windows at HH:45
TONO_WINDOW_SECONDS = 120  # ±2-minute window

# ── Logging ──────────────────────────────────────────────────
APP_LOG = LOG_DIR / "app.log"
PARSING_ERRORS_LOG = LOG_DIR / "parsing_errors.log"
SYSTEM_HEALTH_LOG = LOG_DIR / "system_health.log"
