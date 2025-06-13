from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F


spark = SparkSession.builder \
    .appName("AirPollutionAnalysis") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.read.csv("s3a://dataproc-base-bucket/air_quality/Air_Quality.csv", header=True, inferSchema=True)

df = df.withColumn("DateTime", F.to_timestamp(F.col("Date"))) \
       .withColumn("DateOnly", F.to_date(F.col("DateTime")))

daily_stats = df.groupBy("City", "DateOnly").agg(
    F.avg("CO").alias("Avg_CO"),
    F.max("CO").alias("Max_CO"),
    F.min("CO").alias("Min_CO"),
    F.avg("NO2").alias("Avg_NO2"),
    F.max("NO2").alias("Max_NO2"),
    F.min("NO2").alias("Min_NO2"),
    F.avg("SO2").alias("Avg_SO2"),
    F.avg("O3").alias("Avg_O3"),
    F.avg("PM2_5").alias("Avg_PM2_5"),
    F.avg("PM10").alias("Avg_PM10"),
    F.avg("AQI").alias("Avg_AQI"),
    F.count("*").alias("Hourly_Readings_Count")
).orderBy("City", "DateOnly")

window_spec = Window.partitionBy("City").orderBy("DateOnly")

daily_with_delta = daily_stats.withColumn(
    "CO_Daily_Change", 
    F.col("Avg_CO") - F.lag(F.col("Avg_CO")).over(window_spec)
).withColumn(
    "NO2_Daily_Change", 
    F.col("Avg_NO2") - F.lag(F.col("Avg_NO2")).over(window_spec)
).withColumn(
    "AQI_Daily_Change", 
    F.col("Avg_AQI") - F.lag(F.col("Avg_AQI")).over(window_spec)
)

daily_with_delta.write.parquet("s3a://dataproc-base-bucket/air_quality/daily_with_delta.parquet")
