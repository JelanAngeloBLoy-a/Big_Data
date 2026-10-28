
import json
import os
import subprocess
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, coalesce, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "weather-data"

PROJECT_FOLDER = "/opt/spark-apps"
WRITER_FILE = os.path.join(PROJECT_FOLDER, "mongo_writer.py")


def main():
    spark = (
        SparkSession.builder
        .appName("WeatherKafkaProcessor")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("city", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("value", DoubleType(), True),
        StructField("metric_type", StringType(), True),
        StructField("sensor_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("source", StringType(), True),
    ])

    print("Reading weather messages from Kafka...")

    messages = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    parsed = (
        messages
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )

    processed = (
        parsed
        .withColumn(
            "temperature_c",
            coalesce(col("value"), col("temperature_c"))
        )
        .withColumn(
            "city",
            coalesce(col("city"), col("location"), lit("Unknown"))
        )
        .withColumn(
            "metric_type",
            coalesce(col("metric_type"), lit("temperature"))
        )
        .withColumn(
            "unit",
            coalesce(col("unit"), lit("celsius"))
        )
        .select(
            "city",
            "timestamp",
            "temperature_c",
            "metric_type",
            "sensor_id",
            "unit",
            "source",
        )
        .filter(
            col("timestamp").isNotNull()
            & col("temperature_c").isNotNull()
        )
        .dropDuplicates(["city", "timestamp", "source"])
    )

    processed.show(truncate=False)

    records = [row.asDict() for row in processed.collect()]

    if not records:
        print("No valid records to save.")
        spark.stop()
        return

    # Save the records to a temporary JSON file.

    
    # Save the records to a JSON file in the project folder.
    output_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "processed_records.json",
    )

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(records, file, default=str, indent=2)

    print(f"Saved {len(records)} processed records to:")
    print(output_path)

    spark.stop()
    print("Spark processing completed.")



if __name__ == "__main__":
    main()
