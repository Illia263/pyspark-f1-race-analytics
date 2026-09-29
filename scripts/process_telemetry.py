from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType
from pyspark.sql.types import FloatType
import sys
if __name__ == "__main__":
    spark = SparkSession.builder \
    .appName("f1_analytics") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")\
    .config("spark.driver.memory", "8g")\
    .getOrCreate()
    try:
        laps_df = spark.read.parquet("data/lake/raw/laps/")
        telemetry_df = spark.read.parquet("data/lake/raw/telemetry/")
        print("Showing laps_df firsst 5 rows.....")
        laps_df.show(5)
        print("SHowing telemetry_df first 5 rows .......")
        telemetry_df.show(5)
    except Exception as e:
        print(f"Error occured:  {e}")
        sys.exit(1)
        
  