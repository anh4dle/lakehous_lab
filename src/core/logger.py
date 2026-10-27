import logging
import sys
from pathlib import Path


def get_logger(
    name: str, level: str = "INFO", log_file: Path | None = None
) -> logging.Logger:
    """
    Creates or retrieves a structured logger with console and optional file handlers.

    :param name: Typically pass __name__ to track which module logged the event.
    :param level: Logging level ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL').
    :param log_file: Optional path to save logs on disk.
    """
    # Create or retrieve a logger with the specified name
    logger = logging.getLogger(name)

    # Call get_logger won't add the same handler twice but return the existing one
    if logger.hasHandlers():
        return logger

    # Set log level based on level name, fallback to INFO if not found
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Define log format
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | [%(name)s] | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler 1: Terminal (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler 2: File (Optional)
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
