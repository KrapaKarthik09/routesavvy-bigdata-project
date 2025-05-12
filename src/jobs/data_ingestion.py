import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ────────────────────────────────  1. Spark session
spark = (
    SparkSession.builder
    .appName("NYC_RealTime_Streaming")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", 2)
    .config("spark.driver.memory",  "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# ────────────────────────────────  2. Settings
BOOTSTRAP  = "broker:9093"
CHECKPOINT = "/opt/spark/data/chk"
GROUP_ID   = "nyc-streaming"
RUN_MODE   = os.getenv("RUN_MODE", "live").lower()     # live | historical
OUTPUT_TOPIC = "nyc-combined-data"  # Name of the output Kafka topic

# ────────────────────────────────  3. Schemas (unchanged)
mta_schema = StructType([
    StructField("transit_timestamp", StringType()),
    StructField("transit_mode",      StringType()),
    StructField("station_complex_id",StringType()),
    StructField("station_complex",   StringType()),
    StructField("borough",           StringType()),
    StructField("payment_method",    StringType()),
    StructField("fare_class_category",StringType()),
    StructField("ridership",         DoubleType()),
    StructField("transfers",         DoubleType()),
    StructField("latitude",          DoubleType()),
    StructField("longitude",         DoubleType())
])

traffic_schema = StructType([
    StructField("id", StringType()),
    StructField("speed", DoubleType()),
    StructField("travel_time", IntegerType()),
    StructField("status", IntegerType()),
    StructField("data_as_of", StringType()),
    StructField("link_id", StringType()),
    StructField("borough", StringType()),
    StructField("link_name", StringType()),
    StructField("ingestion_timestamp", DoubleType())
])

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

# ────────────────────────────────  4. Kafka reader helper
def read_kafka(topic, schema, ts_col):
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", topic)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false")
              .option("maxOffsetsPerTrigger", 200_000))
    
    df = (reader.load()
                .select(from_json(col("value").cast("string"), schema).alias("j"))
                .select("j.*")
                .withColumn("event_time", to_timestamp(col(ts_col)))
                .withWatermark("event_time", "15 minutes"))
    return df

mta_df     = read_kafka("mta-subway-data-may-2024",  mta_schema,    "transit_timestamp")
traffic_df = read_kafka("nyc-traffic-data-may-2024", traffic_schema,"data_as_of")
weather_df = read_kafka("weather-data",              weather_schema,"time")

# ────────────────────────────────  5. Join
joined = (mta_df.join(traffic_df, ["borough", "event_time"], "leftOuter")
                 .join(weather_df, ["event_time"], "leftOuter")
                 .select("event_time","station_complex","borough",
                         "ridership","transfers",
                         "speed","travel_time",
                         "temperature_2m","precipitation",
                         "wind_speed_10m","cloud_cover","weather_description"))

# ────────────────────────────────  6. Prepare data for Kafka output
# Convert the joined data to JSON format for Kafka
kafka_output_df = joined.select(
    to_json(struct("*")).alias("value")
)

# ────────────────────────────────  7. Sink configuration
# Set up the writer for Kafka output
writer = (kafka_output_df.writeStream
          .outputMode("append")
          .format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("topic", OUTPUT_TOPIC)
          .option("checkpointLocation", CHECKPOINT + "/kafka-output"))

# Add console output for debugging (optional)
console_writer = (joined.writeStream
                  .outputMode("append")
                  .format("console")
                  .option("truncate", "false")
                  .option("checkpointLocation", CHECKPOINT + "/console"))

# ────────────────────────────────  8. Start the queries
if RUN_MODE == "historical":
    # Back-fill then exit
    kafka_query = writer.trigger(once=True).start()
    # Optional console output for debugging
    console_query = console_writer.trigger(once=True).start()
else:
    # Continuous processing
    kafka_query = writer.trigger(processingTime="30 seconds").start()
    # Optional console output for debugging
    console_query = console_writer.trigger(processingTime="30 seconds").start()

# Wait for termination
kafka_query.awaitTermination()
