from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, lag
import pyspark.sql.functions as F
import sys
from pyspark.sql.window import Window
import shutil
import os
if __name__ == "__main__":
    spark = SparkSession.builder \
    .config("spark.driver.memory", "4g")\
    .getOrCreate()
    try:
        file_path = "/opt/airflow/data/lake/silver/silver_telemetry"
        df = spark.read.parquet(file_path)
    except Exception as e:
        print(f"error occured {e}")
        sys.exit(1)
    filtering_df = df.filter(F.col("nGear").between(4, 6) & (F.col("Speed") > 150))
    avg_speed_df = filtering_df.groupBy("year", "track", "winner", "fastest_lap_driver", "TyreCompound", "TyreLife").agg(
        F.avg("Speed").alias("avg_speed")
    )

    window_spec = Window.partitionBy("year", "track", "winner", "fastest_lap_driver", "TyreCompound").orderBy("TyreLife")

    tyres_degradation_df = avg_speed_df.withColumn(
        "previous_lap_speed",
        F.lag("avg_speed", 1).over(window_spec)
    )\
    .withColumn(
        "speed_drop",
        F.col("previous_lap_speed") - F.col("avg_speed")
    )
    tyres_degradation_df = tyres_degradation_df.withColumn(
        "avg_speed", F.round("avg_speed", 2)
    ).withColumn(
        "previous_lap_speed", F.round("previous_lap_speed", 2)
    ).withColumn(
        "speed_drop", F.round("speed_drop", 2)
    )
    shutil.rmtree("/opt/airflow/data/lake/gold/gold_telemetry", ignore_errors=True)
    tyres_degradation_df.write \
        .mode("overwrite")\
        .parquet("/opt/airflow/data/lake/gold/gold_telemetry")
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT', '5432')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_password = os.environ.get('DB_PASSWORD')

    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }
    tyres_degradation_df.write \
        .jdbc(url=jdbc_url,
            table="race_and_tyre_performance",
            mode="append",
            properties=connection_properties      
        )
