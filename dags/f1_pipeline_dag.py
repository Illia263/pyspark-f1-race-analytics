from datetime import datetime, timedelta
from decimal import Decimal
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator #type: ignore
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator #type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import subprocess
import os
from airflow.sensors.filesystem import FileSensor #type: ignore
logger = logging.getLogger(__name__)
def run_spark_with_db_env(script_path):

    hook = PostgresHook(postgres_conn_id="postgres_conn_id")
    conn_info = hook.get_connection(hook.postgres_conn_id)
    
    env = os.environ.copy()
    env['DB_HOST'] = conn_info.host
    env['DB_PORT'] = str(conn_info.port) if conn_info.port else "5432"
    env['DB_NAME'] = conn_info.schema
    env['DB_USER'] = conn_info.login
    env['DB_PASSWORD'] = conn_info.password
    
    logger.info(f"Running spark script: {script_path}")
    try: 
        result = subprocess.run([
            "spark-submit",
            "--packages", "org.postgresql:postgresql:42.6.0",
            script_path
        ], env=env, check=True, capture_output=True, text=True)
        logger.info(f"=== STDOUT ===\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"=== STDOUT ===\n{e.stdout}")
        logger.error(f"=== STDERR ===\n{e.stderr}")
        raise e
@dag(
    dag_id="f1_data_analytics",
    schedule=None,
    max_active_runs=1,
    catchup=False,
    tags=["f1", "telemetry"]
)
def telemetry_pipeline():
    check_db_alive = PostgresOperator(
        task_id = "check_db_alive",
        postgres_conn_id="postgres_conn_id",
        sql="SELECT 1;"
    )
   
    @task(retries=3,
    retry_delay=timedelta(seconds=1),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(seconds=60))
    def silver_script():
        run_spark_with_db_env("/opt/airflow/scripts/process_silver_telemetry.py")
    
    middle_scripts_sensor = FileSensor(
        
        task_id="wait_for_data_sensor",
        filepath="/opt/airflow/data/silver/silver_telemetry/_SUCCESS",
        fs_conn_id="fs_default", 
        poke_interval=30,        
        timeout=600,         
        mode="poke"              
    )
    @task()
    def gold_script():
        run_spark_with_db_env("/opt/airflow/scripts/process_gold_telemetry.py")

    check_db_alive >> silver_script() >> middle_scripts_sensor >> gold_script()
f1_pipeline = telemetry_pipeline()        

        