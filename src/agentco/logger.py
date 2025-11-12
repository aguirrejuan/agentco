"""
Loguru configuration for Sentra
Handles log rotation and file management
"""

import sys
from pathlib import Path

from loguru import logger


def setup_logging():
    """Setup Agentco logging with file rotation"""
    # Configure loguru
    logger.remove()  # Remove default handler

    # Add console handler
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )

    # Add file handler with rotation
    logger.add(
        rotation="10 MB",  # Rotate when file reaches 10MB
        retention="1 week",  # Keep logs for 1 week
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        enqueue=True,  # Thread-safe logging
    )

    return logger


# Export logger instance
logger = setup_logging()
