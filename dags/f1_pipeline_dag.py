from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator #type: ignore
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator #type: ignore
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import subprocess
import os
from airflow.models.param import Param #type: ignore
from airflow.exceptions import AirflowSkipException
logger = logging.getLogger(__name__)
def run_spark_with_db_env(script_path, year, track):

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
            script_path,
            "--year", str(year),
            "--track", str(track),
        ], env=env, check=True, capture_output=True, text=True)
        logger.info(f"=== STDOUT ===\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"=== STDOUT ===\n{e.stdout}")
        logger.error(f"=== STDERR ===\n{e.stderr}")
        raise e
@dag(
    dag_id="f1_data_analytics",
    schedule=None,
    start_date=datetime(2022,1,1),
    max_active_runs=1,
    catchup=False,
    tags=["f1", "telemetry"],
    params={
        "track" : Param("Monza", type="string", description="Enter a track name"),
        "year" : Param(2022, type="integer", description="Enter a year of the season")
    }
)
def telemetry_pipeline():
    
    
    @task
    def validate_data(**kwargs): 
        import fastf1
        params = kwargs['params']
        year = int(params['year'])
        track = str(params['track']).strip()
        try:
            event = fastf1.get_event(year, track)
            return {"year" : year, "track" : track}
        except ValueError:
            error_msg = f"no data for selected track ({track}) and/or for the selected year ({year})"
            print(error_msg)
            raise AirflowSkipException
    
    
    
    fetching = BashOperator(
        task_id="fetching",
        bash_command="python /opt/airflow/scripts/fetch_f1_data.py --year {{ params.year }} --track '{{ params.track }}'"
    )
    

   
    @task(retries=3,
    retry_delay=timedelta(seconds=1),
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(seconds=60))
    def silver_script(**kwargs):
        params = kwargs["params"]
        logger.info(f"Running silver pyspark script")
        script_path = "/opt/airflow/scripts/process_silver_telemetry.py"
        try:
            result = subprocess.run([
                "spark-submit",
                "--driver-memory", "8g",
                script_path,
                "--year", str(params["year"]),
                "--track", str(params["track"]),
            ], check=True, capture_output=True, text=True)
            logger.info(f"=== STDOUT ===\n{result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"=== STDOUT ===\n{e.stdout}")
            logger.error(f"=== STDERR ===\n{e.stderr}")
            raise e


        
    clear_old_data = PostgresOperator(
        task_id="clear_old_data",
        postgres_conn_id ="postgres_conn_id",
        sql = """
            CREATE TABLE IF NOT EXISTS race_and_tyre_performance(
                year INTEGER,
                track VARCHAR,
                winner VARCHAR,
                fastest_lap_driver VARCHAR,
                TyreCompound VARCHAR,
                TyreLife FLOAT,
                avg_speed FLOAT,
                previous_lap_speed FLOAT,
                DriverNumber VARCHAR,
                speed_drop FLOAT,
                UNIQUE (year, track, DriverNumber, TyreCompound, TyreLife)
            );
            DELETE FROM race_and_tyre_performance
            WHERE track = '{{ params.track }}' AND year = {{ params.year }};
"""
    )
        
    @task()
    def gold_script(**kwargs):
        params = kwargs["params"]
        run_spark_with_db_env(
            "/opt/airflow/scripts/process_gold_telemetry.py",
            params["year"],
            params["track"],
        )

    validate_data() >> fetching >> silver_script() >> clear_old_data >> gold_script()
f1_pipeline = telemetry_pipeline()        

        