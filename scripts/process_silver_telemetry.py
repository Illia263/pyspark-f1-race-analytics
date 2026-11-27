from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, lag
import pyspark.sql.functions as F
import sys
from pyspark.sql.window      import Window
import os
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--track", type=str, required=True)
    args = parser.parse_args()

    bucket_name = os.getenv("GCS_BUCKET_NAME")
    spark = SparkSession.builder \
    .appName("f1_analytics") \
    .config("spark.driver.memory", "8g")\
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/airflow/gcs-key.json") \
    .getOrCreate()
    
    
    try:
        laps_df = spark.read \
            .option("basePath", f"gs://{bucket_name}/raw/laps/") \
            .parquet(f"gs://{bucket_name}/raw/laps/year={args.year}/track={args.track}")
        telemetry_df = spark.read \
            .option("basePath", (f"gs://{bucket_name}/raw/telemetry/")) \
            .parquet(f"gs://{bucket_name}/raw/telemetry/year={args.year}/track={args.track}")
    except Exception as e:
        print(f"Error occured:  {e}")
        sys.exit(1)

    
    telemetry = telemetry_df.alias("t")
    laps = laps_df.alias("l")
    join_cond = [
        F.col("t.year") == F.col("l.year"),
        F.col("t.track") ==  F.col("l.track"),
        F.col("t.SessionTime") >= F.col('l.LapStartTime'),
        F.col('t.SessionTime') <= F.col('l.Time'),
        F.col('t.DriverNumber') == F.col('l.DriverNumber')
    ]
    df_join = telemetry.join(
        broadcast(laps),
        on=join_cond,
        how="inner"
    )


    final_df = df_join.select(
    F.col("l.winner"),
    F.col("l.fastest_lap_driver"),
    F.col("t.DriverNumber"),
    F.col("l.LapNumber"),
    F.col("l.Compound").alias("TyreCompound"),
    F.col("l.TyreLife"),
    F.col("t.SessionTime"),
    F.col("t.Speed"),
    F.col("t.RPM"),
    F.col("t.nGear"),
    F.col("t.Throttle"),
    F.col("t.Brake"),
    F.col("t.X"),
    F.col("t.Y"),
    F.col("l.year"),
    F.col("l.track")
)
    window_spec = Window.partitionBy("year", "track", "DriverNumber", "LapNumber").orderBy("SessionTime")
    df_with_history = final_df.withColumn(
        "previous_sector",
        F.lag("SessionTime", 1).over(window_spec)
    )
    time_diff_df = df_with_history.withColumn(
        "time_delta",
        F.col("SessionTime") - F.col("previous_sector")

    )
    distance_df = time_diff_df.withColumn(
        
        "speed_ms",
        F.col("Speed") / 3.6)\
        .withColumn(
            "time_s",
            F.col("time_delta") / 1000000000
        )\
        .withColumn(
            "distance_delta",
            F.col("speed_ms") * F.col("time_s")
        )
    distance_running_total_df = distance_df.withColumn(
        "total_dist_so_far",
        F.sum("distance_delta").over(window_spec)
    )
    
    distance_running_total_df.write \
        .mode("overwrite") \
        .partitionBy("year", "track") \
        .parquet(f"gs://{bucket_name}/silver/silver_telemetry")
     