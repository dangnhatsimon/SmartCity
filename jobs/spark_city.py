from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from config.config import configuration
import logging


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

KAFKA_BROKER = "broker:29092"
S3_BUCKET = "s3a://spark-kafka-smart-city"


def read_kafka_topic(spark, topic, schema):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "2 minutes")
    )


def stream_writer(input, checkpoint_location, output, mode="append"):
    return (
        input.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output)
        .outputMode(mode)
        .start()
    )


def main():
    spark = (
        SparkSession.builder.appName("SmartCity")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.11.469")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    vehicle_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("fuel_type", StringType(), True)
        ]
    )

    gps_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("vehicle_type", StringType(), True)
        ]
    )

    traffic_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("camera_id", StringType(), True),
            StructField("snapshot", StringType(), True)
        ]
    )

    weather_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("weather_condition", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("air_quality_index", DoubleType(), True)
        ]
    )

    emergency_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("incident_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("type", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )

    vehicle_df = read_kafka_topic(spark, "vehicle_data", vehicle_schema).alias("vehicle")
    gps_df = read_kafka_topic(spark, "gps_data", gps_schema).alias("gps")
    traffic_df = read_kafka_topic(spark, "traffic_data", traffic_schema).alias("traffic")
    weather_df = read_kafka_topic(spark, "weather_data", weather_schema).alias("weather")
    emergency_df = read_kafka_topic(spark, "emergency_data", emergency_schema).alias("emergency")

    query_vehicle = stream_writer(
        input=vehicle_df,
        checkpoint_location=f"{S3_BUCKET}/checkpoints/vehicle_data",
        output=f"{S3_BUCKET}/data/vehicle_data"
    )
    query_gps = stream_writer(
        input=gps_df,
        checkpoint_location=f"{S3_BUCKET}/checkpoints/gps_data",
        output=f"{S3_BUCKET}/data/gps_data"
    )
    query_traffic = stream_writer(
        input=traffic_df,
        checkpoint_location=f"{S3_BUCKET}/checkpoints/traffic_data",
        output=f"{S3_BUCKET}/data/traffic_data"
    )
    query_weather = stream_writer(
        input=weather_df,
        checkpoint_location=f"{S3_BUCKET}/checkpoints/weather_data",
        output=f"{S3_BUCKET}/data/weather_data"
    )
    query_emergency = stream_writer(
        input=emergency_df,
        checkpoint_location=f"{S3_BUCKET}/checkpoints/emergency_data",
        output=f"{S3_BUCKET}/data/emergency_data"
    )

    query_emergency.awaitTermination()


if __name__ == "__main__":
    main()
    
# docker exec -it smartcity-spark-master-1 spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark_city.py