import pendulum
import os
from dotenv import load_dotenv


load_dotenv()

MONGO_DB_NAME =  os.getenv("MONGO_DB") 

MONGO_COLLECTIONS = [
    "isin_basic_info",
    "isin_detailed_info",
    "isin_company_info",
    "isin_rta_info"
]

POSTGRES_TABLES = [
    "company_info",
    "isin_company_map",
    "isin_basic_info",
    "isin_detailed_info",
    "rta_info",
    "isin_rta_map"
    # "migration_logs"  # Uncomment if used for ETL logging
]
BATCH_SIZE = 100
TEST_MODE_LIMIT = 2

# ETL Configuration for isin_profile_transform_dag
ETL_CONFIG = {
    'transform_dag': {
        'start_date': pendulum.datetime(2025, 9, 19, 11, 12, tz="UTC"),  # 04:42 PM IST = 11:12 AM UTC
        'schedule_interval': '30 0 * * *',  # Daily at 6 AM IST (12:30 AM UTC)
    }
}