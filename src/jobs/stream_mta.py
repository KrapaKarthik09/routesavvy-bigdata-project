#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, to_json, struct

# ────────────────────────────────────────── ①  Schema ──────────────────────────────────────────
schema = StructType([
    StructField("transit_timestamp", TimestampType()),
    StructField("transit_mode",       StringType()),
    StructField("station_complex_id", StringType()),
    StructField("station_complex",    StringType()),
    StructField("borough",            StringType()),
    StructField("payment_method",     StringType()),
    StructField("fare_class_category",StringType()),
    StructField("ridership",          DoubleType()),
    StructField("transfers",          DoubleType()),
    StructField("latitude",           DoubleType()),
    StructField("longitude",          DoubleType()),
    StructField("georeference", StructType([
        StructField("type",        StringType()),
        StructField("coordinates", ArrayType(DoubleType()))
    ]))
])

# ────────────────────────────────────────── ②  I/O settings  ───────────────────────────────────
SRC_TOPIC   = "mta-subway-data-may-2024"
DST_TOPIC   = "mta-subway-clean-2"
BOOTSTRAP   = "broker:9093"
CHECKPOINT  = "/opt/spark/data/_chk_mta_k2k"
OFFSET_MODE = "earliest"        # hard-coded “historical” run

spark = (
    SparkSession.builder
        .appName("mta_stream_k2k_sql")
        .getOrCreate()
)

# ────────────────────────────────────────── ③  Read from Kafka ────────────────────────────────
raw_df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", BOOTSTRAP)
         .option("subscribe", SRC_TOPIC)
         .option("startingOffsets", OFFSET_MODE)
         .load()
)

parsed_df = (
    raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json("json_str", schema).alias("data"))
        .select("data.*")
)

# Register the stream as a SQL temp view
parsed_df.createOrReplaceTempView("mta_raw")

# ────────────────────────────────────────── ④  SQL transformation ─────────────────────────────
clean_sql = """
WITH cleaned AS (
  SELECT
      station_complex_id,
      lower(station_complex)           AS station_complex,
      lower(borough)                   AS borough,
      transit_mode,
      transit_timestamp,
      /* ----------- cleanse numeric fields ----------- */
      CAST(regexp_replace(trim(ridership ), ',', '') AS double) AS ridership,
      CAST(regexp_replace(trim(transfers ), ',', '') AS double) AS transfers,
      latitude,
      longitude
  FROM mta_raw
  WHERE station_complex_id IS NOT NULL
)
SELECT
    station_complex_id,
    station_complex,
    borough,
    transit_mode,
    year(transit_timestamp)                              AS year,
    month(transit_timestamp)                             AS month,
    dayofmonth(transit_timestamp)                        AS day,
    hour(transit_timestamp)                              AS hour,

    /* use cleansed numbers, fall back to 0.0 only if *still* null */
    COALESCE(ridership , 0.0)                            AS ridership,
    COALESCE(transfers , 0.0)                            AS transfers,

    CASE WHEN COALESCE(ridership ,0.0) > 0
         THEN COALESCE(transfers ,0.0) /
              COALESCE(ridership ,1.0)
         ELSE 0
    END                                                  AS transfer_ratio,

    latitude,
    longitude,

    CASE
        WHEN hour(transit_timestamp) BETWEEN 7  AND 9
          OR hour(transit_timestamp) BETWEEN 16 AND 19 THEN 1
        ELSE 0
    END                                                  AS peak_hour,

    transit_timestamp
FROM cleaned


"""

clean_df = spark.sql(clean_sql)

# ────────────────────────────────────────── ⑤  Write back to Kafka ─────────────────────────────
(
    clean_df
      .selectExpr(
          "CAST(station_complex_id AS STRING) AS key",
          "to_json(struct(*))                 AS value"
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
