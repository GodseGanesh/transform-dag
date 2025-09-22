from config.logging_config import setup_logging

def log_message(logger, level, message, extra=None):
    """Log a message with optional extra fields."""
    if extra:
        message = f"{message} | {extra}"
    logger.log(level, message)

def log_migration_status(logger, isin, collection, status, details=None):
    """Log migration status for an ISIN."""
    extra = f"ISIN={isin}, collection={collection}, message={status}"
    if details:
        extra += f" | Input: {details}"
    log_message(logger, logging.DEBUG if status.startswith("Skipped") else logging.INFO, extra)