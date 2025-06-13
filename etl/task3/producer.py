#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    df = spark.read.csv("s3a://dataproc-base-bucket/air_quality/Air_Quality.csv", header=True, inferSchema=True)

    df = df.select(
        to_json(struct([df[col] for col in df.columns])).alias("value")
    )

    df.write.format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-m0hj26smhguo41ar.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=someuser "
                "password=somestrongpassword "
                ";") \
        .save()

if __name__ == "__main__":
    main()
