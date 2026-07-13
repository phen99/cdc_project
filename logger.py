import logging
import os
from logging.handlers import TimedRotatingFileHandler

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "cdc.log")

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

# ── Formatter ────────────────────────────────────────────────────────────────
_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ── File handler (rotates daily, keeps 30 days) ──────────────────────────────
_file_handler = TimedRotatingFileHandler(
    filename=LOG_FILE,
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8",
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(_formatter)

# ── Console handler ──────────────────────────────────────────────────────────
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(_formatter)

# ── Root logger configuration ────────────────────────────────────────────────
logging.basicConfig(level=logging.DEBUG, handlers=[_file_handler, _console_handler])


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the given module name.

    Usage:
        logger = get_logger(__name__)
        logger.info("Engine started")
        logger.error("Connection failed", exc_info=True)
    """
    return logging.getLogger(name)
