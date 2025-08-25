import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WeatherStreamProcessor:
    def __init__(self):
        # Initialize Spark Session
        self.spark = (
            SparkSession.builder.appName("WeatherStreamProcessor")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")

        # Kafka configuration
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "weather-data")

        logger.info("Spark Weather Stream Processor initialized")

    def define_schema(self):
        """Define the schema for weather data"""
        return StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("city", StringType(), True),
                StructField("country", StringType(), True),
                StructField("temperature", DoubleType(), True),
                StructField("feels_like", DoubleType(), True),
                StructField("humidity", IntegerType(), True),
                StructField("pressure", IntegerType(), True),
                StructField("description", StringType(), True),
                StructField("wind_speed", DoubleType(), True),
                StructField("wind_direction", IntegerType(), True),
                StructField("cloudiness", IntegerType(), True),
                StructField("visibility", DoubleType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ]
        )

    def create_kafka_stream(self):
        """Create Kafka streaming DataFrame"""
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.kafka_servers)
            .option("subscribe", self.topic)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )

    def process_stream(self):
        """Process the weather data stream"""
        # Create Kafka stream
        kafka_stream = self.create_kafka_stream()

        # Parse JSON data
        weather_schema = self.define_schema()

        parsed_stream = kafka_stream.select(
            col("key").cast("string").alias("city_key"),
            from_json(col("value").cast("string"), weather_schema).alias("weather"),
            col("timestamp").alias("kafka_timestamp"),
        ).select("city_key", "weather.*", "kafka_timestamp")

        # Add processing timestamp
        enriched_stream = (
            parsed_stream.withColumn("processing_time", current_timestamp())
            .withColumn("event_time", to_timestamp(col("timestamp")))
            .withColumn("temp_fahrenheit", (col("temperature") * 9 / 5) + 32)
            .withColumn(
                "temp_category",
                when(col("temperature") < 0, "Freezing")
                .when(col("temperature") < 10, "Cold")
                .when(col("temperature") < 20, "Cool")
                .when(col("temperature") < 30, "Warm")
                .otherwise("Hot"),
            )
        )

        return enriched_stream

    def create_aggregations(self, stream):
        """Create windowed aggregations"""
        # 10-minute tumbling window aggregations
        windowed_agg = (
            stream.withWatermark("event_time", "20 minutes")
            .groupBy(
                window(col("event_time"), "20 minutes"), col("city"), col("country")
            )
            .agg(
                avg("temperature").alias("avg_temperature"),
                max("temperature").alias("max_temperature"),
                min("temperature").alias("min_temperature"),
                avg("humidity").alias("avg_humidity"),
                avg("pressure").alias("avg_pressure"),
                avg("wind_speed").alias("avg_wind_speed"),
                count("*").alias("record_count"),
            )
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("city"),
                col("country"),
                round(col("avg_temperature"), 2).alias("avg_temp_c"),
                round((col("avg_temperature") * 9 / 5) + 32, 2).alias("avg_temp_f"),
                col("max_temperature").alias("max_temp_c"),
                col("min_temperature").alias("min_temp_c"),
                round(col("avg_humidity"), 1).alias("avg_humidity_pct"),
                round(col("avg_pressure"), 1).alias("avg_pressure_hpa"),
                round(col("avg_wind_speed"), 2).alias("avg_wind_speed_mps"),
                col("record_count"),
            )
        )

        return windowed_agg

    def start_processing(self):
        """Start the stream processing"""
        try:
            # Process main stream
            processed_stream = self.process_stream()

            # Console output for real-time monitoring
            console_query = (
                processed_stream.writeStream.outputMode("append")
                .format("console")
                .option("truncate", "false")
                .option("numRows", "20")
                .trigger(processingTime="30 seconds")
                .start()
            )

            # Aggregated data output
            aggregated_stream = self.create_aggregations(processed_stream)

            aggregation_query = (
                aggregated_stream.writeStream.outputMode("update")
                .format("console")
                .option("truncate", "false")
                .option("numRows", "10")
                .trigger(processingTime="60 seconds")
                .start()
            )

            # File output (optional)
            file_query = (
                processed_stream.writeStream.outputMode("append")
                .format("json")
                .option("path", "./output/weather_data")
                .option("checkpointLocation", "./checkpoints/weather")
                .trigger(processingTime="1 minutes")
                .start()
            )

            logger.info("Stream processing started successfully")
            logger.info("Monitoring streams... Press Ctrl+C to stop")

            # Wait for termination
            console_query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
        finally:
            self.spark.stop()


if __name__ == "__main__":
    processor = WeatherStreamProcessor()
    processor.start_processing()
