from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, lag
import pyspark.sql.functions as F
import sys
from pyspark.sql.window      import Window
import shutil
if __name__ == "__main__":
    spark = SparkSession.builder \
    .appName("f1_analytics") \
    .config("spark.driver.memory", "8g")\
    .getOrCreate()
    try:
        laps_df = spark.read.parquet("/opt/airflow/data/lake/raw/laps/")
        telemetry_df = spark.read.parquet("/opt/airflow/data/lake/raw/telemetry/")
    except Exception as e:
        print(f"Error occured:  {e}")
        sys.exit(1)
    telemetry = telemetry_df.alias("t")
    laps = laps_df.alias("l")
    join_cond = [
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
    window_spec = Window.partitionBy("DriverNumber", "LapNumber").orderBy("SessionTime")
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
    shutil.rmtree("opt/airflow/data/lake/silver/silver_telemetry", ignore_errors=True)
    distance_running_total_df.write \
        .mode("overwrite") \
        .parquet("opt/airflow/data/lake/silver/silver_telemetry")
     