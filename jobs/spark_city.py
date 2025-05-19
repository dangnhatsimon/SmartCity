from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql.types import StringType, StructField, DataType
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from conf.config import configuration
import logging
from pyspark.conf import SparkConf


logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    level=logging.DEBUG
)

KAFKA_BROKER = "broker:29092"
S3_BUCKET = "s3a://spark-kafka-smart-city"


def read_kafka_topic(
    spark: SparkSession,
    topic: str,
    schema: DataType,
    broker: str
) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "2 minutes")
    )


def stream_writer(
    input: DataFrame,
    checkpoint_location: str,
    output: str,
    mode: str = "append"
):
    return (
        input.writeStream
        .format("parquet")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output)
        .outputMode(mode)
        .start()
    )


def main():
    access_key = configuration.get("AWS_ACCESS_KEY")
    secret_key = configuration.get("AWS_SECRET_KEY")
    spark = (
        SparkSession.builder.appName("SmartCity")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.11.469")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.driver.cores", 2)
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "1g")
        .config("spark.submit.deployMode", "client")
        .config("spark.log.level", "ALL")
        .getOrCreate()
    )

    sc = spark.sparkContext
    # sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    # sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    # sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

    sc.setLogLevel("WARN")

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

    vehicle_df = read_kafka_topic(spark, "vehicle_data", vehicle_schema, KAFKA_BROKER).alias("vehicle")
    gps_df = read_kafka_topic(spark, "gps_data", gps_schema, KAFKA_BROKER).alias("gps")
    traffic_df = read_kafka_topic(spark, "traffic_data", traffic_schema, KAFKA_BROKER).alias("traffic")
    weather_df = read_kafka_topic(spark, "weather_data", weather_schema, KAFKA_BROKER).alias("weather")
    emergency_df = read_kafka_topic(spark, "emergency_data", emergency_schema, KAFKA_BROKER).alias("emergency")

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
    
# docker exec -it smartcity-spark-master spark-submit --master spark://smartcity-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark_city.py