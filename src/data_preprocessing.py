from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, hour, dayofweek, month, year, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging
import os
offset_mode = "earliest" if os.getenv("RUN_MODE") == "historical" else "latest"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPreprocessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("RouteSavvy-DataPreprocessing") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .getOrCreate()

    def preprocess_mta_data(self, df):
        """Preprocess MTA ridership data"""
        try:
            # Convert timestamp and create time-based features
            processed_df = df \
                .withColumn("timestamp", to_timestamp(col("time"))) \
                .withColumn("hour", hour(col("timestamp"))) \
                .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                .withColumn("month", month(col("timestamp"))) \
                .withColumn("year", year(col("timestamp")))

            # Calculate average ridership by station and time
            station_stats = processed_df.groupBy("station_id", "hour", "day_of_week") \
                .agg(
                    avg("ridership").alias("avg_ridership"),
                    stddev("ridership").alias("stddev_ridership")
                )

            return processed_df, station_stats
        except Exception as e:
            logger.error(f"Error preprocessing MTA data: {e}")
            return None, None

    def preprocess_weather_data(self, df):
        """Preprocess weather data"""
        try:
            # Extract relevant weather features
            weather_df = df.select(
                col("timestamp"),
                col("temperature"),
                col("humidity"),
                col("precipitation"),
                col("wind_speed")
            )

            # Calculate weather impact scores
            weather_df = weather_df.withColumn(
                "weather_impact_score",
                (col("precipitation") * 0.4 + 
                 col("wind_speed") * 0.3 + 
                 col("humidity") * 0.3)
            )

            return weather_df
        except Exception as e:
            logger.error(f"Error preprocessing weather data: {e}")
            return None

    def combine_features(self, mta_df, weather_df):
        """Combine MTA and weather features"""
        try:
            # Join datasets on timestamp
            combined_df = mta_df.join(
                weather_df,
                mta_df.timestamp == weather_df.timestamp,
                "left"
            )

            return combined_df
        except Exception as e:
            logger.error(f"Error combining features: {e}")
            return None

    def process_streaming_data(self):
        """Process streaming data from Kafka"""
        try:
            # Read MTA data stream
            mta_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "mta_ridership") \
                .load()

            # Read weather data stream
            weather_stream = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", "weather") \
                .load()

            # Process streams
            processed_mta = self.preprocess_mta_data(mta_stream)
            processed_weather = self.preprocess_weather_data(weather_stream)
            
            # Combine features
            final_stream = self.combine_features(processed_mta[0], processed_weather)

            # Write to output topic
            query = final_stream \
                .writeStream \
                .outputMode("append") \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("topic", "processed_data") \
                .start()

            query.awaitTermination()

        except Exception as e:
            logger.error(f"Error in stream processing: {e}")

if __name__ == "__main__":
    preprocessor = DataPreprocessor()
    preprocessor.process_streaming_data()
