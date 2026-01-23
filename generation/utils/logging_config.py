import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "votes_generator.log"


def setup_logging(level=logging.INFO):
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(formatter)

    logging.basicConfig(
        level=level, handlers=[console_handler, file_handler], force=True
    )
