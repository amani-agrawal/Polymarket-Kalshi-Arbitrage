"""
logger.py — Centralised loguru logging configuration.

Logs to both stdout (coloured) and arb.log (plain text, rotating at 50 MB).
All modules call get_logger(__name__) to obtain a named child logger.
"""

import sys
from loguru import logger


def setup_logging(log_level: str = "INFO") -> None:
    """
    Configure loguru sinks: console + rotating file.

    Should be called once at daemon startup (in main.py). Subsequent calls
    are safe — they remove existing sinks first to avoid duplicates.

    Args:
        log_level: Minimum severity to emit. E.g. "DEBUG", "INFO", "WARNING".
    """
    logger.remove()  # Drop default stderr sink

    # Console sink — coloured, human-readable
    logger.add(
        sys.stdout,
        level=log_level,
        colorize=True,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        enqueue=True,  # thread-safe
    )

    # File sink — plain text, 50 MB rotation, 14-day retention
    logger.add(
        "arb.log",
        level=log_level,
        rotation="50 MB",
        retention="14 days",
        compression="gz",
        enqueue=True,
        format=(
            "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
            "{name}:{function}:{line} - {message}"
        ),
    )


def get_logger(name: str):
    """
    Return a loguru logger bound to the given module name.

    Usage:
        from logger import get_logger
        log = get_logger(__name__)
        log.info("hello")

    Args:
        name: Typically __name__ of the calling module.

    Returns:
        A loguru logger instance with the 'name' binding.
    """
    return logger.bind(name=name)
