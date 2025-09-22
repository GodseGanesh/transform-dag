
import logging
from logging.handlers import RotatingFileHandler
import os

def setup_logging():
    """Configure logging for the ETL process with file and stdout handlers."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Ensure logs directory exists
    log_dir = "/opt/airflow/dags/logs"
    os.makedirs(log_dir, exist_ok=True)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, "isin_profile_transform.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5  # Keep 5 backup files
    )
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # Stream handler for stdout
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s"
    )
    stream_handler.setFormatter(stream_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
