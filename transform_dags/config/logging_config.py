import logging
from logging.handlers import RotatingFileHandler
import os
import sys

def setup_logging(logger_name="isin_etl", log_dir="/opt/airflow/dags/logs"):
    """Configure logging with stdout (for Airflow) and optional file rotation."""
    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        return logger  # Avoid adding duplicate handlers

    logger.setLevel(logging.DEBUG)  # Capture everything

    # Ensure logs directory exists
    os.makedirs(log_dir, exist_ok=True)

    # File handler with rotation (optional, keeps history)
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, f"{logger_name}.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Stream handler for Airflow logs
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
    )
    stream_handler.setFormatter(stream_formatter)
    logger.addHandler(stream_handler)

    return logger

