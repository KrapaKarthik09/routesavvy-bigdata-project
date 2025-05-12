import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, to_json, struct, col, year, month, dayofmonth, hour, when, lower, coalesce

# ---------- ① schema ----------
schema = StructType([
    StructField("transit_timestamp", TimestampType()),
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
        StructField("coordinates", ArrayType(DoubleType()))
    ]))
])

# ---------- ② I/O settings ----------
SRC_TOPIC  = "mta-subway-data-may-2024"
DST_TOPIC  = "mta-subway-clean"
BOOTSTRAP  = "broker:9093"
CHECKPOINT = "/opt/spark/data/_chk_mta_k2k"

# Choose earliest or latest based on env
offset_mode = "earliest"

spark = (SparkSession.builder
         .appName("mta_stream_k2k")
         .getOrCreate())

# ---------- ③ read from Kafka ----------
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", SRC_TOPIC)
       .option("startingOffsets", offset_mode)
       .load())

parsed = (raw
          .selectExpr("CAST(value AS STRING) AS json_str")
          .select(from_json("json_str", schema).alias("data"))
          .select("data.*"))

parsed = (parsed
    # Extract date components
    .withColumn("year", year("transit_timestamp"))
    .withColumn("month", month("transit_timestamp"))
    .withColumn("day", dayofmonth("transit_timestamp"))
    .withColumn("hour", hour("transit_timestamp"))
    
    # Add peak hour indicator
    .withColumn("peak_hour", when(col("hour").between(7, 9) | col("hour").between(16, 19), 1).otherwise(0))
    
    # Normalize text fields
    .withColumn("station_complex", lower(col("station_complex")))
    .withColumn("borough", lower(col("borough")))
    
    # Handle missing ridership and transfers gracefully
    .withColumn("ridership", coalesce(col("ridership"), col("transfers"), col("ridership")).cast(DoubleType()))
    .withColumn("transfers", coalesce(col("transfers"), col("ridership"), col("transfers")).cast(DoubleType()))
    
    # Calculate transfer ratio safely
    .withColumn("transfer_ratio", when(col("ridership") > 0, col("transfers") / col("ridership")).otherwise(0))
    

    
    # Final column selection
    .select(
        "station_complex_id",
        "station_complex",
        "borough",
        "transit_mode",
        "year", "month", "day", "hour",
        "ridership", "transfers", "transfer_ratio",
        "latitude", "longitude",
        "peak_hour",
        "transit_timestamp"
    )
)

# ---------- ⑤ Transform and Write to Kafka ----------
(
    parsed.drop("georeference")  # Drop unwanted nested fields if needed
    .selectExpr(
        "CAST(station_complex_id AS STRING) AS key",
        "to_json(struct(*)) AS value"
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", DST_TOPIC)
    .option("checkpointLocation", CHECKPOINT)
    .outputMode("append")
    .start()
    .awaitTermination()
)
