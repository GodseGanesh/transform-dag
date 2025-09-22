# isin_profile_transform.py
import logging
import hashlib
import json
import pendulum
from config.database_config import get_mongo_client, get_postgres_connection
from config.etl_config import MONGO_DB_NAME, MONGO_COLLECTIONS, POSTGRES_TABLES, BATCH_SIZE, TEST_MODE_LIMIT
from utils.logging_utils import setup_logging
from mappings.postgres_mappings import map_to_postgres

logger = setup_logging()



# def run_isin_profile_transform(test_mode=False):
#     """Run ETL for ISIN profile from MongoDB → PostgreSQL."""
#     try:
#         # Mongo setup
#         mongo_client = get_mongo_client()
#         db = mongo_client[MONGO_DB_NAME]

#         if test_mode:
#             logger.info("Running ETL in TEST MODE. Extra logging enabled.")

#         # Fetch unique ISINs
#         isins = set()
#         for collection in MONGO_COLLECTIONS:
#             cursor = db[collection].find({}, {"ISIN_CODE": 1})
#             isins.update(doc["ISIN_CODE"] for doc in cursor if "ISIN_CODE" in doc)

#             if test_mode:
#                 doc_count = db[collection].count_documents({})
#                 logger.info(f"Fetched {doc_count} docs from collection {collection}")

#         if not isins:
#             logger.info("No ISINs found. Exiting ETL.")
#             return

#         logger.info(f"Found {len(isins)} unique ISINs to process")

#         # Process in batches
#         for i in range(0, len(isins), BATCH_SIZE):
#             batch = list(isins)[i:i + BATCH_SIZE]
#             if test_mode:
#                 logger.info(f"Original batch size: {len(batch)}. Applying TEST_MODE_LIMIT={TEST_MODE_LIMIT}")
#                 batch = batch[:TEST_MODE_LIMIT]
#                 logger.info(f"Trimmed batch size (test mode): {len(batch)}")
#                 logger.debug(f"Batch ISINs: {batch}")

#             for isin in batch:
#                 # Fetch all collection docs for ISIN
#                 data = {}
#                 for collection in MONGO_COLLECTIONS:
#                     doc = db[collection].find_one({"ISIN_CODE": isin})
#                     if doc:
#                         data[collection] = doc
#                         if test_mode:
#                             logger.debug(f"Fetched doc for ISIN {isin} from {collection}: {doc}")

#                 if not data:
#                     logger.warning(f"No data found for ISIN {isin}. Skipping.")
#                     continue

#                 # Clean + map
#                 mapped_postgres_data = map_to_postgres(data)
#                 if test_mode:
#                     logger.debug(f"Mapped Postgres data for ISIN {isin}: {json.dumps(mapped_postgres_data, indent=2, default=str)}")

#                 # Hash for incremental load
#                 data_str = json.dumps(mapped_postgres_data, sort_keys=True, default=str)
#                 data_hash = hashlib.sha256(data_str.encode()).hexdigest()
#                 if test_mode:
#                     logger.info(f"Computed hash for ISIN {isin}: {data_hash}")

#                 # Upsert into Postgres
#                 conn = get_postgres_connection()
#                 try:
#                     with conn.cursor() as cur:
#                         for table in POSTGRES_TABLES:
#                             table_data = mapped_postgres_data.get(table)
#                             if not table_data:
#                                 if test_mode:
#                                     logger.debug(f"No data for table {table} and ISIN {isin}")
#                                 continue

#                             # Check existing hash
#                             cur.execute(f"SELECT data_hash FROM {table} WHERE isin_code = %s", (isin,))
#                             existing = cur.fetchone()
#                             existing_hash = existing[0] if existing else None

#                             if existing_hash == data_hash:
#                                 logger.info(f"ISIN {isin} in {table}: skipped (no changes).")
#                                 continue

#                             # Insert/Update row
#                             cur.execute(
#                                 f"""
#                                 INSERT INTO {table} (isin_code, data_hash, last_updated)
#                                 VALUES (%s, %s, %s)
#                                 ON CONFLICT (isin_code)
#                                 DO UPDATE SET data_hash = EXCLUDED.data_hash, last_updated = EXCLUDED.last_updated
#                                 """,
#                                 (isin, data_hash, pendulum.now())
#                             )
#                             logger.info(f"ISIN {isin} in {table}: inserted/updated.")

#                     conn.commit()
#                 except Exception as e:
#                     logger.error(f"Error processing ISIN {isin}: {e}")
#                     conn.rollback()
#                 finally:
#                     conn.close()

#     except Exception as e:
#         logger.exception(f"ETL failed: {e}")
#         raise
#     finally:
#         mongo_client.close()


def run_isin_profile_transform(test_mode=False):
    """Run ETL for ISIN profile from MongoDB → PostgreSQL with detailed logging."""
    logger.info("=== Starting ISIN profile ETL ===")
    mongo_client = None

    try:
        # 1️⃣ MongoDB setup
        logger.info("Connecting to MongoDB...")
        mongo_client = get_mongo_client()
        db = mongo_client[MONGO_DB_NAME]
        logger.info(f"Connected to MongoDB database: {MONGO_DB_NAME}")

        if test_mode:
            logger.info("TEST MODE ENABLED: Extra logging active.")

        # 2️⃣ Fetch unique ISINs
        isins = set()
        for collection in MONGO_COLLECTIONS:
            logger.info(f"Fetching ISINs from collection: {collection}")
            cursor = db[collection].find({}, {"ISIN_CODE": 1})
            count = 0
            for doc in cursor:
                if "ISIN_CODE" in doc:
                    isins.add(doc["ISIN_CODE"])
                    count += 1
            logger.info(f"Fetched {count} ISINs from collection {collection}")
            if test_mode:
                total_docs = db[collection].count_documents({})
                logger.info(f"Total documents in collection {collection}: {total_docs}")

        if not isins:
            logger.warning("No ISINs found. Exiting ETL.")
            return
        logger.info(f"Total unique ISINs to process: {len(isins)}")

        # 3️⃣ Process in batches
        for i in range(0, len(isins), BATCH_SIZE):
            batch = list(isins)[i:i + BATCH_SIZE]
            if test_mode:
                logger.info(f"Processing batch {i // BATCH_SIZE + 1} with original size {len(batch)}")
                batch = batch[:TEST_MODE_LIMIT]
                logger.info(f"Batch trimmed to TEST_MODE_LIMIT={TEST_MODE_LIMIT}, size={len(batch)}")
                logger.debug(f"Batch ISINs: {batch}")

            for isin in batch:
                logger.info(f"Processing ISIN: {isin}")

                # 3a️⃣ Fetch all documents for this ISIN
                data = {}
                for collection in MONGO_COLLECTIONS:
                    doc = db[collection].find_one({"ISIN_CODE": isin})
                    if doc:
                        data[collection] = doc
                        logger.debug(f"Fetched document from {collection} for ISIN {isin}: {doc}")
                if not data:
                    logger.warning(f"No data found for ISIN {isin}. Skipping...")
                    continue

                # 3b️⃣ Map data to Postgres format
                try:
                    mapped_postgres_data = map_to_postgres(data)
                    logger.debug(f"Mapped Postgres data for ISIN {isin}: {json.dumps(mapped_postgres_data, indent=2, default=str)}")
                except Exception as e:
                    logger.error(f"Mapping failed for ISIN {isin}: {e}")
                    continue

                # 3c️⃣ Compute hash for incremental load
                data_str = json.dumps(mapped_postgres_data, sort_keys=True, default=str)
                data_hash = hashlib.sha256(data_str.encode()).hexdigest()
                logger.info(f"Computed hash for ISIN {isin}: {data_hash}")

                # 3d️⃣ Upsert into Postgres
                logger.info(f"Connecting to PostgreSQL for ISIN {isin}...")
                conn = get_postgres_connection()
                try:
                    with conn.cursor() as cur:
                        for table in POSTGRES_TABLES:
                            table_data = mapped_postgres_data.get(table)
                            if not table_data:
                                logger.debug(f"No data for table {table} and ISIN {isin}, skipping...")
                                continue

                            # Check existing hash
                            cur.execute(f"SELECT data_hash FROM {table} WHERE isin_code = %s", (isin,))
                            existing = cur.fetchone()
                            existing_hash = existing[0] if existing else None

                            if existing_hash == data_hash:
                                logger.info(f"ISIN {isin} in {table}: skipped (no changes).")
                                continue

                            # Insert/Update row
                            cur.execute(
                                f"""
                                INSERT INTO {table} (isin_code, data_hash, last_updated)
                                VALUES (%s, %s, %s)
                                ON CONFLICT (isin_code)
                                DO UPDATE SET data_hash = EXCLUDED.data_hash, last_updated = EXCLUDED.last_updated
                                """,
                                (isin, data_hash, pendulum.now())
                            )
                            logger.info(f"ISIN {isin} in {table}: inserted/updated.")
                    conn.commit()
                    logger.debug(f"Postgres transaction committed for ISIN {isin}")
                except Exception as e:
                    logger.error(f"Error inserting/updating ISIN {isin}: {e}")
                    conn.rollback()
                finally:
                    conn.close()
                    logger.debug(f"Postgres connection closed for ISIN {isin}")

        logger.info("=== ETL completed successfully ===")

    except Exception as e:
        logger.exception(f"ETL failed: {e}")
        raise
    finally:
        if mongo_client:
            mongo_client.close()
            logger.debug("MongoDB connection closed")



if __name__ == "__main__":
    run_isin_profile_transform(test_mode=True)
