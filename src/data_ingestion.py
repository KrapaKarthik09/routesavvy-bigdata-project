from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, split, to_timestamp, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, FloatType, IntegerType, LongType, TimestampType
import json

# Create Spark session
spark = SparkSession.builder \
    .appName("NYC_RealTime_Streaming") \
    .master("local[*]") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "localhost:9092"
checkpoint_dir = "/tmp/spark-checkpoints"

# Define schema for MTA data
mta_schema = StructType([
    StructField("transit_timestamp", StringType()),
    StructField("transit_mode", StringType()),
    StructField("station_complex_id", StringType()),
    StructField("station_complex", StringType()),
    StructField("borough", StringType()),
    StructField("payment_method", StringType()),
    StructField("fare_class_category", StringType()),
    StructField("ridership", DoubleType()),
    StructField("transfers", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("georeference", StructType([
        StructField("type", StringType()),
        StructField("coordinates", StructType([
            StructField("longitude", DoubleType()),
            StructField("latitude", DoubleType())
        ]))
    ]))
])

# Define schema for Traffic data
traffic_schema = StructType([
    StructField("id", StringType()),
    StructField("speed", DoubleType()),
    StructField("travel_time", IntegerType()),
    StructField("status", IntegerType()),
    StructField("data_as_of", StringType()),
    StructField("link_id", StringType()),
    StructField("link_points", StringType()),
    StructField("encoded_poly_line", StringType()),
    StructField("encoded_poly_line_lvls", StringType()),
    StructField("owner", StringType()),
    StructField("transcom_id", StringType()),
    StructField("borough", StringType()),
    StructField("link_name", StringType()),
    StructField("ingestion_timestamp", DoubleType())
])

# Define schema for Weather data
weather_schema = StructType([
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("elevation", DoubleType()),
    StructField("timezone", StringType()),
    StructField("timezone_abbreviation", StringType()),
    StructField("producer_timestamp", DoubleType()),
    StructField("location_name", StringType()),
    StructField("query_period", StringType()),
    StructField("time", StringType()),
    StructField("temperature_2m", DoubleType()),
    StructField("precipitation", DoubleType()),
    StructField("wind_speed_10m", DoubleType()),
    StructField("visibility", DoubleType()),
    StructField("cloud_cover", IntegerType()),
    StructField("weather_code", IntegerType()),
    StructField("weather_description", StringType())
])

# Read MTA subway data from Kafka
mta_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "mta-subway-data-may-2024") \
    .option("startingOffsets", "earliest") \
    .load()

mta_df = mta_df.select(from_json(col("value").cast("string"), mta_schema).alias("data")).select("data.*")

# Read NYC traffic data from Kafka
traffic_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "nyc-traffic-data-may-2024") \
    .option("startingOffsets", "earliest") \
    .load()

traffic_df = traffic_df.select(from_json(col("value").cast("string"), traffic_schema).alias("data")).select("data.*")

# Read weather data from Kafka
weather_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "weather-data") \
    .option("startingOffsets", "earliest") \
    .load()

weather_df = weather_df.select(from_json(col("value").cast("string"), weather_schema).alias("data")).select("data.*")

# Convert timestamps for joining
mta_df = mta_df.withColumn("event_time", to_timestamp(col("transit_timestamp")))
traffic_df = traffic_df.withColumn("event_time", to_timestamp(col("data_as_of")))
weather_df = weather_df.withColumn("event_time", to_timestamp(col("time")))

# Join MTA, Traffic, and Weather data on time and location (latitude, longitude)
combined_df = mta_df.join(traffic_df, ["event_time", "borough"], "left") \
    .join(weather_df, ["event_time"], "left") \
    .select(
        "event_time",
        "station_complex",
        "borough",
        "ridership",
        "transfers",
        "speed",
        "travel_time",
        "temperature_2m",
        "precipitation",
        "wind_speed_10m",
        "cloud_cover",
        "weather_description"
    )

# Output to console for testing
query = combined_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", checkpoint_dir) \
    .start()

query.awaitTermination()
