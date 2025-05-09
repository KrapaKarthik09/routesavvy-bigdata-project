from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, window, expr, when, lit, udf, array, 
                                  to_timestamp, struct, current_timestamp, 
                                  from_unixtime, collect_list, size)
from pyspark.sql.types import (StructType, StructField, StringType, FloatType, 
                              IntegerType, MapType, ArrayType, BooleanType)
import math
import json
import time
from datetime import datetime
import sys
import os
from config import (KAFKA, MONGODB, CHECKPOINTS, STREAMING, WEATHER, 
                    TRAFFIC, MOBILITY, GEOSPATIAL)

def create_spark_session():
    """Initialize Spark Session with required dependencies"""
    try:
        return (SparkSession.builder
                .appName("UrbanMobilityOptimizer")
                .config("spark.jars.packages", 
                        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,"
                        "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")
                .getOrCreate())
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        sys.exit(1)

def define_schemas():
    """Define schemas for all data sources"""
    # MTA Service Alerts Schema
    service_alert_schema = StructType([
        StructField("alert_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("update_number", StringType(), True),
        StructField("date", StringType(), True),
        StructField("agency", StringType(), True),
        StructField("status_label", StringType(), True),
        StructField("affected", StringType(), True),
        StructField("header", StringType(), True),
        StructField("description", StringType(), True),
        StructField("ingestion_timestamp", FloatType(), True)
    ])
    
    # MTA Subway Data Schema
    subway_schema = StructType([
        StructField("division", StringType(), True),
        StructField("line", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("stop_name", StringType(), True),
        StructField("complex_id", StringType(), True),
        StructField("constituent_station_name", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("gtfs_stop_id", StringType(), True),
        StructField("daytime_routes", StringType(), True),
        StructField("entrance_type", StringType(), True),
        StructField("entry_allowed", StringType(), True),
        StructField("exit_allowed", StringType(), True),
        StructField("entrance_latitude", StringType(), True),
        StructField("entrance_longitude", StringType(), True),
        StructField("entrance_georeference", MapType(StringType(), 
                                                  ArrayType(FloatType()), True), True)
    ])
    
    # NYC Traffic Data Schema
    traffic_schema = StructType([
        StructField("id", StringType(), True),
        StructField("speed", StringType(), True),
        StructField("travel_time", StringType(), True),
        StructField("status", StringType(), True),
        StructField("data_as_of", StringType(), True),
        StructField("link_id", StringType(), True),
        StructField("link_points", StringType(), True),
        StructField("encoded_poly_line", StringType(), True),
        StructField("encoded_poly_line_lvls", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("transcom_id", StringType(), True),
        StructField("borough", StringType(), True),
        StructField("link_name", StringType(), True),
        StructField("ingestion_timestamp", FloatType(), True)
    ])
    
    # Weather Data Schema
    weather_schema = StructType([
        StructField("latitude", FloatType(), True),
        StructField("longitude", FloatType(), True),
        StructField("current_units", MapType(StringType(), StringType()), True),
        StructField("current", MapType(StringType(), StringType()), True),
        StructField("hourly_units", MapType(StringType(), StringType()), True),
        StructField("hourly", MapType(StringType(), ArrayType(StringType())), True),
        StructField("producer_timestamp", FloatType(), True),
        StructField("location_name", StringType(), True)
    ])
    
    return {
        "service_alert": service_alert_schema,
        "subway": subway_schema,
        "traffic": traffic_schema,
        "weather": weather_schema
    }

# Define UDFs for geospatial calculations
@udf(returnType=FloatType())
def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points using Haversine formula"""
    if not lat1 or not lon1 or not lat2 or not lon2:
        return None
        
    # Convert string coordinates to float if needed
    try:
        lat1, lon1 = float(lat1), float(lon1)
        lat2, lon2 = float(lat2), float(lon2)
    except (ValueError, TypeError):
        return None
    
    # Earth's radius in kilometers
    R = GEOSPATIAL['EARTH_RADIUS_KM']
    
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    
    a = math.sin(dlat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    
    distance = R * c
    return distance

@udf(returnType=FloatType())
def calculate_weather_impact(weather_code, precipitation, visibility):
    """Calculate travel impact factor based on weather conditions"""
    try:
        weather_code = int(float(weather_code)) if weather_code else 0
        precipitation = float(precipitation) if precipitation else 0
        visibility = float(visibility) if visibility else 10000
    except:
        return 1.0  # Default no impact
    
    # Base impact factor
    impact = 1.0
    
    # Weather code impact (severe weather increases factor)
    if weather_code in WEATHER['CODES']['THUNDERSTORM']:
        impact += WEATHER['IMPACT_FACTORS']['THUNDERSTORM']
    elif weather_code in WEATHER['CODES']['HEAVY_RAIN']:
        impact += WEATHER['IMPACT_FACTORS']['HEAVY_RAIN']
    elif weather_code in WEATHER['CODES']['SNOW']:
        impact += WEATHER['IMPACT_FACTORS']['SNOW']
    elif weather_code in WEATHER['CODES']['FOG']:
        impact += WEATHER['IMPACT_FACTORS']['FOG']
    
    # Precipitation impact
    if precipitation > WEATHER['PRECIPITATION_THRESHOLDS']['HEAVY']:
        impact += WEATHER['PRECIPITATION_IMPACTS']['HEAVY']
    elif precipitation > WEATHER['PRECIPITATION_THRESHOLDS']['MODERATE']:
        impact += WEATHER['PRECIPITATION_IMPACTS']['MODERATE']
    elif precipitation > WEATHER['PRECIPITATION_THRESHOLDS']['LIGHT']:
        impact += WEATHER['PRECIPITATION_IMPACTS']['LIGHT']
    
    # Visibility impact
    if visibility < WEATHER['VISIBILITY_THRESHOLDS']['VERY_LOW']:
        impact += WEATHER['VISIBILITY_IMPACTS']['VERY_LOW']
    elif visibility < WEATHER['VISIBILITY_THRESHOLDS']['LOW']:
        impact += WEATHER['VISIBILITY_IMPACTS']['LOW']
    
    return impact

@udf(returnType=ArrayType(StringType()))
def extract_route_services(daytime_routes):
    """Extract array of train services from daytime_routes string"""
    if not daytime_routes:
        return []
    return daytime_routes.strip().split()

@udf(returnType=BooleanType())
def is_route_affected(route_services, affected_route):
    """Check if any of the station's route services is affected by alert"""
    if not route_services or not affected_route:
        return False
    
    return affected_route in route_services

def main():
    print("Starting Urban Mobility Optimizer Spark Job")
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        schemas = define_schemas()
        
        # Read from Kafka topics
        service_alerts = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA['BOOTSTRAP_SERVERS'])
            .option("subscribe", KAFKA['TOPICS']['SERVICE_ALERTS'])
            .option("startingOffsets", KAFKA['START_OFFSETS'])
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json("json", schemas["service_alert"]).alias("data"))
            .select("data.*")
            .withColumn("alert_timestamp", to_timestamp(col("date")))
            .withColumn("alert_severity", when(col("status_label") == "delay", 3)
                                   .when(col("status_label") == "detour", 2)
                                   .otherwise(1))
            .withWatermark("alert_timestamp", STREAMING['WATERMARKS']['ALERTS']))
        
        subway_data = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA['BOOTSTRAP_SERVERS'])
            .option("subscribe", KAFKA['TOPICS']['SUBWAY_DATA'])
            .option("startingOffsets", KAFKA['START_OFFSETS'])
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json("json", schemas["subway"]).alias("data"))
            .select("data.*")
            .withColumn("latitude", col("entrance_latitude").cast(FloatType()))
            .withColumn("longitude", col("entrance_longitude").cast(FloatType()))
            .withColumn("route_services", extract_route_services(col("daytime_routes")))
            .withColumn("processing_time", current_timestamp()))
        
        traffic_data = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA['BOOTSTRAP_SERVERS'])
            .option("subscribe", KAFKA['TOPICS']['TRAFFIC_DATA'])
            .option("startingOffsets", KAFKA['START_OFFSETS'])
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json("json", schemas["traffic"]).alias("data"))
            .select("data.*")
            .withColumn("speed_float", col("speed").cast(FloatType()))
            .withColumn("travel_time_float", col("travel_time").cast(FloatType()))
            .withColumn("traffic_timestamp", to_timestamp(col("data_as_of")))
            .withColumn("congestion_level", 
                       when(col("speed_float") < TRAFFIC['CONGESTION_THRESHOLDS']['SEVERE'], 
                           TRAFFIC['CONGESTION_LEVELS']['SEVERE'])
                      .when(col("speed_float") < TRAFFIC['CONGESTION_THRESHOLDS']['MODERATE'], 
                           TRAFFIC['CONGESTION_LEVELS']['MODERATE'])
                      .when(col("speed_float") < TRAFFIC['CONGESTION_THRESHOLDS']['LIGHT'], 
                           TRAFFIC['CONGESTION_LEVELS']['LIGHT'])
                      .otherwise(TRAFFIC['CONGESTION_LEVELS']['NORMAL']))
            .withWatermark("traffic_timestamp", STREAMING['WATERMARKS']['TRAFFIC']))
        
        weather_data = (spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA['BOOTSTRAP_SERVERS'])
            .option("subscribe", KAFKA['TOPICS']['WEATHER_DATA'])
            .option("startingOffsets", KAFKA['START_OFFSETS'])
            .load()
            .selectExpr("CAST(value AS STRING) as json")
            .select(from_json("json", schemas["weather"]).alias("data"))
            .select("data.*")
            .withColumn("weather_timestamp", to_timestamp(from_unixtime(col("producer_timestamp"))))
            .withColumn("temperature", col("current.temperature_2m").cast(FloatType()))
            .withColumn("precipitation", col("current.precipitation").cast(FloatType()))
            .withColumn("wind_speed", col("current.wind_speed_10m").cast(FloatType()))
            .withColumn("weather_code", col("current.weather_code").cast(IntegerType()))
            .withColumn("weather_description", col("current.weather_description"))
            .withColumn("visibility", expr("cast(hourly.visibility[0] as float)"))
            .withColumn("travel_impact_factor", calculate_weather_impact(
                col("weather_code"), col("precipitation"), col("visibility")))
            .withWatermark("weather_timestamp", STREAMING['WATERMARKS']['WEATHER']))
        
        # Process and join data streams to create transit network model
        
        # 1. Process subway stations to create network nodes
        subway_nodes_query = (subway_data
            .writeStream
            .format("mongodb")
            .option("checkpointLocation", CHECKPOINTS['SUBWAY_NODES'])
            .option("forceDeleteTempCheckpointLocation", "true")
            .option("spark.mongodb.connection.uri", MONGODB['URI'])
            .option("spark.mongodb.database", MONGODB['DATABASE'])
            .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['SUBWAY_STATIONS'])
            .start())
        
        # 2. Process service alerts affecting subway and bus services
        service_alerts_query = (service_alerts
            .writeStream
            .format("mongodb")
            .option("checkpointLocation", CHECKPOINTS['SERVICE_ALERTS'])
            .option("forceDeleteTempCheckpointLocation", "true")
            .option("spark.mongodb.connection.uri", MONGODB['URI'])
            .option("spark.mongodb.database", MONGODB['DATABASE'])
            .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['TRANSIT_ALERTS'])
            .start())
        
        # 3. Process traffic data for road network performance
        traffic_query = (traffic_data
            .writeStream
            .format("mongodb")
            .option("checkpointLocation", CHECKPOINTS['TRAFFIC_DATA'])
            .option("forceDeleteTempCheckpointLocation", "true")
            .option("spark.mongodb.connection.uri", MONGODB['URI'])
            .option("spark.mongodb.database", MONGODB['DATABASE'])
            .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['ROAD_TRAFFIC'])
            .start())
        
        # 4. Process weather data for environmental conditions
        weather_query = (weather_data
            .writeStream
            .format("mongodb")
            .option("checkpointLocation", CHECKPOINTS['WEATHER_DATA'])
            .option("forceDeleteTempCheckpointLocation", "true")
            .option("spark.mongodb.connection.uri", MONGODB['URI'])
            .option("spark.mongodb.database", MONGODB['DATABASE'])
            .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['WEATHER_CONDITIONS'])
            .start())
        
        # 5. Join subway data with traffic and alerts to create mobility scores
        def process_batch(batch_df, batch_id):
            """Process batch by joining with static data and calculating mobility scores"""
            try:
                # Check if batch is empty
                if batch_df.count() == 0:
                    print(f"Batch {batch_id} is empty, skipping processing")
                    return
                
                print(f"Processing batch {batch_id} with {batch_df.count()} records")
                
                # Get current alerts
                alerts_df = spark.read.format("mongodb").option("spark.mongodb.connection.uri", 
                                                             MONGODB['URI']) \
                                   .option("spark.mongodb.database", MONGODB['DATABASE']) \
                                   .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['TRANSIT_ALERTS']) \
                                   .load()
                
                # Get current weather
                weather_df = spark.read.format("mongodb").option("spark.mongodb.connection.uri", 
                                                              MONGODB['URI']) \
                                    .option("spark.mongodb.database", MONGODB['DATABASE']) \
                                    .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['WEATHER_CONDITIONS']) \
                                    .load() \
                                    .orderBy(col("weather_timestamp").desc()) \
                                    .limit(1)
                
                # Get current traffic
                traffic_df = spark.read.format("mongodb").option("spark.mongodb.connection.uri", 
                                                              MONGODB['URI']) \
                                    .option("spark.mongodb.database", MONGODB['DATABASE']) \
                                    .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['ROAD_TRAFFIC']) \
                                    .load()
                
                # Join subway stations with relevant service alerts
                stations_with_alerts = batch_df.alias("stations").join(
                    alerts_df.alias("alerts"),
                    expr("array_contains(stations.route_services, alerts.affected)"),
                    "left_outer"
                ).groupBy("stations.station_id", "stations.stop_name", "stations.daytime_routes", 
                         "stations.latitude", "stations.longitude", "stations.borough") \
                 .agg(
                    collect_list(struct("alerts.alert_id", "alerts.status_label", "alerts.alert_severity", 
                                      "alerts.header", "alerts.description")).alias("alerts")
                 )
                
                # Get weather impact if available
                weather_impact = 1.0
                if weather_df.count() > 0:
                    weather_impact = weather_df.select("travel_impact_factor").collect()[0][0]
                
                # Calculate mobility scores based on alerts and weather
                scored_stations = stations_with_alerts.withColumn(
                    "mobility_score", 
                    when(size("alerts") > 0, 
                        MOBILITY['BASE_SCORE'] - (size("alerts") * MOBILITY['ALERT_SCORE_PENALTY'])) \
                    .otherwise(MOBILITY['BASE_SCORE'])
                ).withColumn(
                    "weather_adjusted_score",
                    col("mobility_score") / lit(weather_impact)
                )
                
                # Add timestamp for this batch processing
                timestamp_df = scored_stations.withColumn(
                    "processed_at", 
                    lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )
                
                # Write results to MongoDB
                timestamp_df.write \
                    .format("mongodb") \
                    .option("spark.mongodb.connection.uri", MONGODB['URI']) \
                    .option("spark.mongodb.database", MONGODB['DATABASE']) \
                    .option("spark.mongodb.collection", MONGODB['COLLECTIONS']['ROUTE_OPTIMIZATION']) \
                    .mode("append") \
                    .save()
                
                print(f"Successfully processed batch {batch_id} and wrote {timestamp_df.count()} records to MongoDB")
                
            except Exception as e:
                print(f"Error processing batch {batch_id}: {e}")
        
        # Create integrated batch processing stream
        integrated_query = (subway_data
            .writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", CHECKPOINTS['INTEGRATED'])
            .option("forceDeleteTempCheckpointLocation", "true")
            .trigger(processingTime=STREAMING['TRIGGER_INTERVAL'])
            .start())
        
        # Wait for termination
        print("All streaming queries started, waiting for termination...")
        integrated_query.awaitTermination()
        
    except KeyboardInterrupt:
        print("Shutting down due to keyboard interrupt...")
    except Exception as e:
        print(f"Error in main execution: {e}")
    finally:
        print("Stopping Spark session...")
        try:
            # Clean up resources
            if 'spark' in locals():
                spark.stop()
        except Exception as e:
            print(f"Error stopping Spark session: {e}")

if __name__ == "__main__":
    main()
