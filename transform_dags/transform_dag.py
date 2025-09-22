
# transform_dag.py (Airflow DAG entry)
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from scripts.isin_profile_transform import run_isin_profile_transform

with DAG(
    dag_id="isin_profile_transform_dag",
    start_date=pendulum.datetime(2025, 9, 19, tz="Asia/Kolkata"),
    schedule_interval="@daily", 
    catchup=False,
    tags=["isin", "transform", "postgres"]
) as dag:

    def run_isin_profile_task(test_mode=False):
        """Wrapper for Airflow task"""
        run_isin_profile_transform(test_mode=test_mode)

    isin_profile_etl = PythonOperator(
        task_id="isin_profile_etl",
        python_callable=run_isin_profile_task,
        op_kwargs={"test_mode": False},   # Set True if testing
    )

