# ─────────────────────────────────────────────────────────────
# Relaxed join: MTA  ⇽±5 min⇾  Traffic   (ignore weather)
# ─────────────────────────────────────────────────────────────
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)

# 1 ── Spark session  (cluster master + resources)
spark = (
    SparkSession.builder
    .appName("NYC_Streaming_RelaxedJoin")
    .master("spark://spark-master:7077")
    .config("spark.executor.memory", "4g")
    .config("spark.executor.cores", 2)
    .config("spark.driver.memory",  "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

# 2 ── Settings
BOOTSTRAP  = "broker:9093"
CHECKPOINT = "/opt/spark/data/chk_relaxed"
GROUP_ID   = "nyc-streaming-relaxed"
RUN_MODE   = os.getenv("RUN_MODE", "live").lower()      # live | historical

# 3 ── Schemas
mta_schema = StructType([
    StructField("transit_timestamp",  StringType()),
    StructField("transit_mode",       StringType()),
    StructField("station_complex_id", StringType()),
    StructField("station_complex",    StringType()),
    StructField("borough",            StringType()),
    StructField("payment_method",     StringType()),
    StructField("fare_class_category",StringType()),
    StructField("ridership",          DoubleType()),
    StructField("transfers",          DoubleType()),
    StructField("latitude",           DoubleType()),
    StructField("longitude",          DoubleType())
])

traffic_schema = StructType([
    StructField("id",            StringType()),
    StructField("speed",         DoubleType()),
    StructField("travel_time",   IntegerType()),
    StructField("status",        IntegerType()),
    StructField("data_as_of",    StringType()),
    StructField("link_id",       StringType()),
    StructField("borough",       StringType()),
    StructField("link_name",     StringType()),
    StructField("ingestion_timestamp", DoubleType())
])

# 4 ── Kafka reader helper
def read_kafka(topic, schema, ts_col):
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", BOOTSTRAP)
              .option("subscribe", topic)
              .option("group.id", GROUP_ID)
              .option("startingOffsets", "earliest")
              .option("failOnDataLoss", "false"))
    if RUN_MODE == "historical":
        reader = reader.option("endingOffsets", "latest")
    return (reader.load()
            .select(from_json(col("value").cast("string"), schema).alias("j"))
            .select("j.*")
            .withColumn("event_time", to_timestamp(col(ts_col)))
            .withWatermark("event_time", "15 minutes"))

mta_df     = read_kafka("mta-subway-data-may-2024",  mta_schema,    "transit_timestamp")
traffic_df = read_kafka("nyc-traffic-data-may-2024", traffic_schema,"data_as_of")

# 5 ── Relaxed join: same borough AND |Δt| ≤ 5 min
joined = (
    mta_df.join(
        traffic_df,
        (mta_df.borough == traffic_df.borough) &
        (traffic_df.event_time.between(
            expr("event_time - interval 5 minutes"),
            expr("event_time + interval 5 minutes"))),
        "leftOuter"                            # keep every MTA record
    )
    .select(
        mta_df.event_time.alias("event_time"),
        "station_complex", "borough",
        "ridership", "transfers",
        "speed", "travel_time", "link_name"
    )
)

# 6 ── Sink to console
writer = (joined.writeStream.outputMode("append")
          .format("console").option("truncate","false").option("numRows","20")
          .option("checkpointLocation", CHECKPOINT))

query = (writer.trigger(once=True).start() if RUN_MODE == "historical"
         else writer.trigger(processingTime="30 seconds").start())

query.awaitTermination()
