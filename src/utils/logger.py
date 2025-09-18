"""
Logging configuration for ExpiryTrack
"""
import logging
import sys
from pathlib import Path
from datetime import datetime

from loguru import logger
from ..config import config

def setup_logging():
    """Configure application logging"""

    # Remove default loguru handler
    logger.remove()

    # Create logs directory
    config.LOGS_DIR.mkdir(exist_ok=True)

    # Add file handler with rotation
    log_file = config.LOGS_DIR / f"expirytrack_{datetime.now().strftime('%Y%m%d')}.log"
    logger.add(
        log_file,
        rotation="500 MB",
        retention="30 days",
        level=config.LOG_LEVEL,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {module}:{function}:{line} - {message}",
        backtrace=True,
        diagnose=True
    )

    # Add console handler with color
    logger.add(
        sys.stdout,
        level=config.LOG_LEVEL,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{module}</cyan> - <level>{message}</level>",
        colorize=True
    )

    # Configure standard logging to use loguru
    class InterceptHandler(logging.Handler):
        def emit(self, record):
            # Get corresponding Loguru level if it exists
            try:
                level = logger.level(record.levelname).name
            except ValueError:
                level = record.levelno

            # Find caller from where originated the logged message
            frame, depth = logging.currentframe(), 2
            while frame.f_code.co_filename == logging.__file__:
                frame = frame.f_back
                depth += 1

            logger.opt(depth=depth, exception=record.exc_info).log(
                level, record.getMessage()
            )

    # Replace standard logging with loguru
    logging.basicConfig(handlers=[InterceptHandler()], level=0)

    # Suppress noisy libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logger.info("Logging configured successfully")
    return logger