import logging
import os
import sys
from logging.handlers import RotatingFileHandler

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(ROOT_DIR, "votes_generator.log")


def setup_logging(level: int = logging.INFO):
    """
    Configures application-wide logging.

    This setup enables:
    - Console logging to standard output
    - File logging with rotation to prevent unbounded file growth

    Logs are formatted consistently across handlers.

    Args:
        level (int, optional): Logging level (e.g., logging.INFO, logging.DEBUG).
            Defaults to logging.INFO.
    """
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
