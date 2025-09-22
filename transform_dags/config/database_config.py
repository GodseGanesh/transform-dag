# utils/database_config.py
from pymongo import MongoClient
import psycopg2
from dotenv import load_dotenv
import os
from config.logging_config import setup_logging

load_dotenv()

logger = setup_logging()  # Use centralized logger


def get_mongo_client():
    """Return a MongoDB client instance."""
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        logger.error("MONGO_URI environment variable not set")
        raise ValueError("MONGO_URI environment variable not set")

    logger.info("Creating MongoDB client")
    try:
        client = MongoClient(mongo_uri)
        client.admin.command("ping")  # Test connection
        logger.info("MongoDB client connected successfully")
        return client
    except Exception as e:
        logger.exception(f"Failed to connect to MongoDB: {e}")
        raise


def get_mongo_db(client):
    """Return the MongoDB database instance."""
    db_name = os.getenv("MONGO_DB")
    if not db_name:
        logger.error("MONGO_DB environment variable not set")
        raise ValueError("MONGO_DB environment variable not set")

    logger.info(f"Accessing MongoDB database: {db_name}")
    return client[db_name]


def get_postgres_connection():
    """Return a PostgreSQL connection."""
    pg_dsn = os.getenv("PG_DSN")
    if not pg_dsn:
        logger.error("PG_DSN environment variable not set")
        raise ValueError("PG_DSN environment variable not set")

    logger.info("Creating PostgreSQL connection")
    try:
        conn = psycopg2.connect(pg_dsn)
        logger.info("PostgreSQL connection established successfully")
        return conn
    except Exception as e:
        logger.exception(f"Failed to connect to PostgreSQL: {e}")
        raise
