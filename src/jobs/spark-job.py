#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, to_json, struct, col
import time

# ────────────────────────────────────────── ① Schema ──────────────────────────────────────────
# Changed to StringType for initial parsing, we'll convert later
schema = StructType([
    StructField("transit_timestamp", TimestampType()),
    StructField("transit_mode", StringType()),
    StructField("station_complex_id", StringType()),
    StructField("station_complex", StringType()),
    StructField("borough", StringType()),
    StructField("payment_method", StringType()),
    StructField("fare_class_category", StringType()),
    StructField("ridership", StringType()),  # Changed to StringType
    StructField("transfers", StringType()),  # Changed to StringType 
    StructField("latitude", StringType()),   # Changed to StringType
    StructField("longitude", StringType()),  # Changed to StringType
    StructField("georeference", StructType([
        StructField("type", StringType()),
        StructField("coordinates", ArrayType(DoubleType()))
    ]))
])

# ────────────────────────────────────────── ② I/O settings ───────────────────────────────────
SRC_TOPIC = "mta-subway-data-may-2024"
BOOTSTRAP = "broker:9093"
CHECKPOINT = f"/opt/spark/data/_chk_mta_mongodb_{int(time.time())}"
OFFSET_MODE = "earliest" # hard-coded "historical" run

# MongoDB connection settings
MONGODB_URI = "mongodb://mongo:27017"
MONGODB_DATABASE = "mta_data"
MONGODB_COLLECTION = "subway_data_clean3"

spark = (
    SparkSession.builder
        .appName("mta_stream_kafka_to_mongodb")
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector:10.0.5")
        .getOrCreate()
)

# ────────────────────────────────────────── ③ Read from Kafka ────────────────────────────────
raw_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", BOOTSTRAP)
         .option("subscribe", SRC_TOPIC)
         .option("startingOffsets", OFFSET_MODE)
         .option("failOnDataLoss", "false")  # Add this to handle offset issues
         .load()
)

# Parse the JSON first
parsed_df = (
    raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json("json_str", schema).alias("data"))
        .select("data.*")
        # Explicitly cast numeric columns here
        .withColumn("ridership", col("ridership").cast("double"))
        .withColumn("transfers", col("transfers").cast("double")) 
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
)

# Register the stream as a SQL temp view
parsed_df.createOrReplaceTempView("mta_raw")

# ────────────────────────────────────────── ④ SQL transformation ─────────────────────────────
clean_sql = """
SELECT
    station_complex_id,
    lower(station_complex) AS station_complex,
    lower(borough) AS borough,
    transit_mode,
    transit_timestamp,
    year(transit_timestamp) AS year,
    month(transit_timestamp) AS month,
    dayofmonth(transit_timestamp) AS day,
    hour(transit_timestamp) AS hour,

    /* Use direct casting since we've already cast in the DataFrame */
    ridership,
    transfers,

    /* Calculate transfer ratio */
    CASE WHEN ridership > 0
         THEN transfers / ridership
         ELSE 0
    END AS transfer_ratio,

    /* Direct use of latitude/longitude */
    latitude,
    longitude,

    /* Simplified peak hour calculation */
    CASE
        WHEN hour(transit_timestamp) BETWEEN 7 AND 9 OR 
             hour(transit_timestamp) BETWEEN 16 AND 19 THEN 1
        ELSE 0
    END AS peak_hour,

    transit_timestamp
FROM mta_raw
WHERE station_complex_id IS NOT NULL
"""

clean_df = spark.sql(clean_sql)

# ────────────────────────────────────────── ⑤ Write to MongoDB ─────────────────────────────────
def write_to_mongodb(batch_df, batch_id):
    if not batch_df.isEmpty():
        # Debug: Print sample data to verify values
        print(f"Writing batch {batch_id}, sample data:")
        batch_df.select("station_complex_id", "ridership", "transfers", 
                         "latitude", "longitude", "peak_hour").show(5)
        
        batch_df.write \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", MONGODB_URI) \
            .option("spark.mongodb.database", MONGODB_DATABASE) \
            .option("spark.mongodb.collection", MONGODB_COLLECTION) \
            .mode("append") \
            .save()

(
    clean_df
        .writeStream
        .foreachBatch(write_to_mongodb)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .start()
        .awaitTermination()
)
