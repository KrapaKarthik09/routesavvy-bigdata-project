from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, hour, dayofweek, month, year, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
import json

# Schema definition remains the same
schema = StructType([
    # Your schema fields remain unchanged
    StructField("transit_timestamp", StringType(), True),
    StructField("transit_mode", StringType(), True),
    StructField("station_complex_id", StringType(), True),
    StructField("station_complex", StringType(), True),
    StructField("borough", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("fare_class_category", StringType(), True),
    StructField("ridership", StringType(), True),
    StructField("transfers", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("georeference", StructType([
        StructField("type", StringType(), True),
        StructField("coordinates", ArrayType(DoubleType()), True)
    ]), True)
])

# Spark session configuration remains the same
spark = SparkSession.builder \
    .appName("MTA Subway Data Processing") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.kryoserializer.buffer.max", "512m") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "10") \
    .config("spark.dynamicAllocation.enabled", "false") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = "broker:9093"
INPUT_TOPIC = "mta-subway-data-may-2024"
OUTPUT_TOPIC_BASE = "mta-subway-analysis"

# Set log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Process in smaller batches to avoid memory issues
spark.sparkContext.setCheckpointDir("/opt/spark/data/")

# FIXED: Create a JSON string with specific large offsets for multiple partitions 
# This handles cases where the topic might have multiple partitions
partition_offsets = {str(i): 10000000 for i in range(10)}  # Assume up to 10 partitions
end_offsets_json = json.dumps({INPUT_TOPIC: partition_offsets})

# Read data from Kafka with specific end offsets
kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", end_offsets_json) \
    .option("failOnDataLoss", "false") \
    .load()

# Rest of your code remains unchanged
# Parse JSON data from Kafka
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .repartition(10)  # Repartition to better distribute data

# Data transformations - optimized to be memory efficient
transformed_df = parsed_df \
    .withColumn("transit_timestamp", to_timestamp(col("transit_timestamp"))) \
    .withColumn("ridership", col("ridership").cast("double")) \
    .withColumn("transfers", col("transfers").cast("double")) \
    .withColumn("latitude", col("latitude").cast("double")) \
    .withColumn("longitude", col("longitude").cast("double")) \
    .withColumn("hour", hour("transit_timestamp")) \
    .withColumn("day_of_week", dayofweek("transit_timestamp")) \
    .withColumn("month", month("transit_timestamp")) \
    .withColumn("year", year("transit_timestamp")) \
    .withColumn("is_weekend", expr("day_of_week IN (1, 7)").cast("int"))

# Register as temp view for SQL queries
transformed_df.createOrReplaceTempView("mta_data")

# Function to write dataframe to Kafka topic
def write_to_kafka(df, topic_suffix):
    output_topic = f"{OUTPUT_TOPIC_BASE}-{topic_suffix}"
    df.selectExpr("CAST(station_complex AS STRING) AS key", "to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", output_topic) \
        .save()
    print(f"Data written to Kafka topic: {output_topic}")

# Example analysis 1: Total ridership by station and borough - with smaller result set
station_ridership = spark.sql("""
    SELECT 
        station_complex, 
        borough, 
        SUM(ridership) AS total_ridership 
    FROM mta_data 
    GROUP BY station_complex, borough 
    ORDER BY total_ridership DESC 
    LIMIT 100
""")

write_to_kafka(station_ridership, "station-ridership")

# Example analysis 2: Payment method distribution - with smaller result set
payment_analysis = spark.sql("""
    SELECT 
        payment_method, 
        fare_class_category, 
        SUM(ridership) AS total_ridership 
    FROM mta_data 
    GROUP BY payment_method, fare_class_category 
    ORDER BY total_ridership DESC 
    LIMIT 50
""")

write_to_kafka(payment_analysis, "payment-analysis")

# Example analysis 3: Hourly patterns - with smaller result set
hourly_patterns = spark.sql("""
    SELECT 
        hour, 
        day_of_week, 
        is_weekend, 
        SUM(ridership) AS total_ridership 
    FROM mta_data 
    GROUP BY hour, day_of_week, is_weekend 
    ORDER BY hour, day_of_week 
    LIMIT 100
""")

write_to_kafka(hourly_patterns, "hourly-patterns")

# Example analysis 4: Borough-level ridership trends - with smaller result set
borough_trends = spark.sql("""
    SELECT 
        borough, 
        year, 
        month, 
        SUM(ridership) AS total_ridership 
    FROM mta_data 
    GROUP BY borough, year, month 
    ORDER BY year, month, borough 
    LIMIT 100
""")

write_to_kafka(borough_trends, "borough-trends")

# Show top 10 stations by ridership
print("Top 10 stations by ridership:")
station_ridership.show(10)

# Stop Spark session
spark.stop()
