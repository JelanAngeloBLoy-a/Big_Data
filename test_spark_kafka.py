
from pyspark.sql import SparkSession

# Start Spark and download the Kafka connector if needed
spark = (
    SparkSession.builder
    .appName("WeatherKafkaTest")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

try:
    print("Reading weather records from Kafka...")

    records = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "weather-data")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    # Convert Kafka's binary message into readable text
    weather = records.selectExpr(
        "CAST(value AS STRING) AS weather_record"
    )

    weather.show(truncate=False)

    print("SUCCESS: Spark read the Kafka records!")

finally:
    spark.stop()
