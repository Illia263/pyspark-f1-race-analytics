from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, lag
import pyspark.sql.functions as F
import sys
from pyspark.sql.window import Window
import shutil

if __name__ == "__main__":
    spark = SparkSession.builder \
    .config("spark.driver.memory", "4g")\
    .getOrCreate()
    try:
        file_path = "data/lake/silver/silver_telemetry"
        df = spark.read.parquet(file_path)
    except Exception as e:
        print(f"error occured {e}")
        sys.exit(1)
    filtering_df = df.filter(F.col("nGear").between(4, 6) & (F.col("Speed") > 150))
    avg_speed_df = filtering_df.groupBy("TyreCompound", "TyreLife").agg(
        F.avg("Speed").alias("avg_speed")
    )

    window_spec = Window.partitionBy("TyreCompound").orderBy("TyreLife")

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
    shutil.rmtree("data/lake/gold/gold_telemetry", ignore_errors=True)
    tyres_degradation_df.write \
        .mode("overwrite")\
        .parquet("data/lake/gold/gold_telemetry")

    # docker exec f1_airflow cat standalone_admin_password.txt 