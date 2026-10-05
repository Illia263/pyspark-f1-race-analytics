# 🏎️ F1 Telemetry Data Pipeline

An analytical data pipeline built with PySpark and Apache Airflow to process high-frequency Formula 1 car telemetry. This project automates the extraction of raw time-series data via the FastF1 API, applies transformations, calculates aggregated tyre degradation metrics, and loads the final insights into a PostgreSQL data mart.

## 🏗️ Pipeline Architecture

The project implements a classic Medallion architecture (Raw -> Silver -> Gold), orchestrated by the `f1_data_analytics` Airflow DAG:

* **Raw Layer (Data Ingestion):** The `fetch_f1_data.py` script downloads general lap statistics and microsecond-level telemetry (speed, coordinates, engine RPM) for a selected Grand Prix. The data is enriched with the names of the race winner and the fastest lap driver, and then saved into the Data Lake in Parquet format, partitioned by year and track.


* **Silver Layer (Processing):** The `process_silver_telemetry.py` script performs a PySpark JOIN between the telemetry and lap datasets to enrich each data point with contextual metadata (driver, tyre compound, current tyre age). Utilizing PySpark window functions, it calculates dynamic time intervals and distance covered across micro-sectors.


* **Gold Layer (Aggregation & Serving):** The `process_gold_telemetry.py` script filters the data for high-speed corners and computes the average speed drop as a function of tyre age (`TyreLife`) across different tyre compounds (`TyreCompound`). The aggregated results are loaded into a target PostgreSQL table named `race_and_tyre_performance` using a JDBC driver.



## 🛠️ Technology Stack

* **Orchestration:** Apache Airflow 2.9.2


* **Data Processing:** Apache Spark (PySpark), Pandas


* **Data Source:** FastF1 API


* **Storage:** PostgreSQL 15, Local File System (Data Lake)


* **Infrastructure:** Docker, Docker Compose (featuring a custom image with Java 17 for PySpark execution)



---

📌 **Note:** Detailed deployment and setup instructions for running this pipeline on a clean machine are provided in a separate file (`DEPLOYMENT.md`).