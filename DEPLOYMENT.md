# **F1 Telemetry Data Pipeline: Complete Deployment Guide**

For running the pipeline on a clean machine, the provided data is almost sufficient. You have the complete infrastructure configuration, including Docker Compose, Dockerfile, Python dependencies, and the Airflow DAG setup. However, to execute the pipeline successfully, you must transfer the actual Python worker scripts (`fetch_f1_data.py`, `process_silver_telemetry.py`, and `process_gold_telemetry.py`) referenced by the DAG into the new environment, as their source code is not included in the current context.


## Pipeline Deployment Steps

### 1. Prepare Directory Structure

On a clean machine with Docker and Docker Compose installed, create the root project directory and subfolders for volume mounting:

```bash
mkdir f1_project && cd f1_project
mkdir dags scripts data logs

```

### 2. Distribute Files

Place your project files into the created structure:

* **Root Directory (`f1_project/`):** Place `docker-compose.yaml`, `Dockerfile`, and `requirements.txt` here.


* **`dags/` Directory:** Place your `f1_pipeline_dag_2.py` file here (it is recommended to rename it to `f1_pipeline_dag.py`).


* **`scripts/` Directory:** Place your Python execution scripts (`fetch_f1_data.py`, `process_silver_telemetry.py`, and `process_gold_telemetry.py`) here.



### 3. Build and Start the Environment

From the root directory, start the build process. Docker will download the base Airflow image, install Java 17 required for PySpark, and install the necessary Python libraries:

```bash
docker-compose up --build -d

```

### 4. Access Airflow UI

The `f1_airflow` container runs the `airflow standalone` command, which initializes the database and automatically generates a password for the `admin` user.

1. Retrieve the admin password by executing this command in your terminal:
```bash
docker exec f1_airflow cat standalone_admin_password.txt

```


2. Open your web browser and navigate to `http://localhost:8080`.


3. Log in using the username `admin` and the password you retrieved.
4. Unpause the `f1_data_analytics` DAG and trigger it. You do not need to manually configure the `postgres_conn_id` connection, as it is already provided via environment variables.



### 5. Verify Results in the Database

The PostgreSQL container maps port `5432` to your host machine. To view the final `race_and_tyre_performance` table, connect using any SQL client (like DBeaver) with the credentials defined in the configuration:

* **Host:** `localhost`
* **Port:** `5432`
* **Database:** `f1_analytics`
* **Username:** `airflow_user`
* **Password:** `f1_data_base`