from confluent_kafka import Producer
import json
import requests
import time

# NYC Department of Transportation traffic information API
# This endpoint provides real-time traffic speed data from sensors throughout NYC
response = requests.get('https://data.cityofnewyork.us/resource/i4gi-tjb9.json')
traffic_data = response.json()

# Create a Kafka producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Topic name
topic = 'nyc-traffic-data'

# Process and publish each record
count = 0
for data in traffic_data:
    # Add timestamp for real-time tracking
    data['ingestion_timestamp'] = time.time()
    
    # Convert the data to JSON string and produce to Kafka
    print(f'Producing traffic data {count}: Speed {data.get("speed", "N/A")} on {data.get("segment_name", "Unknown")}')
    p.produce(topic, json.dumps(data))
    count += 1
    
    # Optional: Call poll to improve performance
    p.poll(0)

# Ensure all messages are sent
p.flush()
print(f"Published {count} traffic records to topic: {topic}")
