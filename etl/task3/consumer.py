#!/usr/bin/env python3

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def create_kafka_consumer():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-stream-app") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider") \
        .getOrCreate()

    schema = StructType([
        StructField("Date", TimestampType()),
        StructField("City", StringType()),
        StructField("CO", DoubleType()),
        StructField("CO2", DoubleType()),
        StructField("NO2", DoubleType()),
        StructField("SO2", DoubleType()),
        StructField("O3", DoubleType()),
        StructField("PM2_5", DoubleType()),
        StructField("PM10", DoubleType()),
        StructField("AQI", DoubleType())
    ])

    stream_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-m0hj26smhguo41ar.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=someuser "
                "password=somestrongpassword "
                ";") \
        .option("startingOffsets", "earliest")\
        .load()

    parsed_df = stream_df \
        .select(
            F.from_json(F.col("value").cast("string"), schema).alias("data")
        ) \
        .select("data.*")
    
    with_watermark = parsed_df.withWatermark("Date", "1 hour") \
        .withColumn("date", F.to_date(F.col("Date")))

    stats_df = with_watermark \
        .groupBy("City", "date") \
        .agg(
            F.avg("CO").alias("avg_co"),
            F.avg("NO2").alias("avg_no2"),
            F.avg("PM2_5").alias("avg_pm25"),
            F.avg("PM10").alias("avg_pm10"),
            F.avg("AQI").alias("avg_aqi"),
            F.max("AQI").alias("max_aqi"),
            F.min("AQI").alias("min_aqi"),
            F.count("*").alias("record_count"),
            F.current_timestamp().alias("processing_time")
        )

    stats_query = stats_df.writeStream \
        .outputMode("update") \
        .trigger(once=True) \
        .format("memory") \
        .queryName("air_quality_stats") \
        .start()

    stats_query.awaitTermination()

    spark.sql("SELECT * FROM air_quality_stats") \
        .write \
        .format("parquet") \
        .partitionBy("date", "City") \
        .save("s3a://dataproc-base-bucket/air_quality/stats/")

if __name__ == "__main__":
    create_kafka_consumer()
