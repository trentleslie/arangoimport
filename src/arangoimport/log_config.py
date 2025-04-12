"""Logging configuration for arangoimport."""

import logging
import sys
from datetime import datetime
from pathlib import Path

# Map log level strings to logging constants
LOG_LEVELS = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}

def setup_logging(level_str: str = 'WARNING') -> None:
    """Configure logging for the application.

    Args:
        level_str: Logging level string (e.g., 'DEBUG', 'INFO'). Default: 'WARNING'
    """
    # Get the corresponding logging level constant, default to WARNING if invalid
    level = LOG_LEVELS.get(level_str.upper(), logging.WARNING)

    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    # Create log filename with timestamp
    log_file = (
        log_dir / f"arangodb_import_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
    )


def get_logger(name: str | None = None) -> logging.Logger:
    """Get a logger instance.

    Args:
        name: Logger name (default: None)

    Returns:
        logging.Logger: Logger instance
    """
    logger = logging.getLogger(name)
    return logger
