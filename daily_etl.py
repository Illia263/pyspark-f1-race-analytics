from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.types import DateType
import sys
import os
from pyspark.sql.types import FloatType
if __name__ == "__main__":
    spark = SparkSession.builder\
    .appName("Daily_ecommerce")\
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")\
    .config("spark.driver.memory", "2g")\
    .getOrCreate()
    try:
        date_obj = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        month_folder = date_obj.strftime("%Y-%b")
        file_path = f"/opt/airflow/data/lake/raw/events/{month_folder}/event_date={sys.argv[1]}/"
        if not os.path.exists(file_path):
            print(f"No data found for {sys.argv[1]}. Skipping gracefully.")
            sys.exit(0)
    except Exception as e:
        print(f"Here is an error {str(e)}!")
        sys.exit(1)
    df = spark.read.parquet(file_path)
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    dim_users_df = spark.read\
        .format("jdbc")\
        .option("url", db_url)\
        .option("dbtable", "dim_users")\
        .option("user", db_user)\
        .option("password", db_password)\
        .option("driver", "org.postgresql.Driver")\
        .load()
    bad_data = (
        F.col("user_id").isNull() |
        (F.col("price").cast(FloatType()) <= 0) |
        (F.col("event_type").isNull()) |
        (F.col("event_type") == "")
    )
    rejected_df = df.filter(bad_data)
    clean_df = df.filter(~bad_data)
    rejected_path = f"/opt/airflow/data/lake/rejected/events/{sys.argv[1]}/"
    rejected_df.write \
        .mode("overwrite") \
        .parquet(rejected_path)
    
    enriched_df = clean_df.join(dim_users_df, on='user_id', how="left")
    grouped_users = enriched_df.groupBy('user_id', 'country').agg(
        F.count(F.when(F.col("event_type") == "view", 1)).alias("total_views"),
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price").cast(FloatType())).otherwise(0)).alias("total_spend"),
        F.count(F.when(F.col("event_type") == "cart", 1)).alias("total_carts"),
        F.count(F.when(F.col("event_type") == "purchase", 1)).alias("total_purchases")
    )
    
    final_df = grouped_users.withColumn("event_date", F.lit(sys.argv[1]).cast(DateType()))
    final_df.write \
    .format("jdbc")\
    .option("url", db_url)\
    .option("dbtable", "daily_user_metrics")\
    .option("user", db_user)\
    .option("password", db_password)\
    .option("driver", "org.postgresql.Driver")\
    .mode("append")\
    .save()