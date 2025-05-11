from confluent_kafka import Producer
import json
import requests
import time

# API token
api_token = "bzKl9z4AM7HbMvrLkmYenPujd"

# Kafka producer configuration (2GB buffer for 8GB RAM)
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'traffic-json-producer',
    'queue.buffering.max.messages': 5000000,
    'queue.buffering.max.kbytes': 2097152,
    'batch.num.messages': 50000,
    'message.timeout.ms': 120000,
    'compression.type': 'gzip',
    'linger.ms': 1000
}

# Create a Kafka producer
p = Producer(producer_config)

# NYC Department of Transportation traffic information API
api_url = 'https://data.cityofnewyork.us/resource/i4gi-tjb9.json'

# Date range for May 2024
start_date = "2024-05-01T00:00:00.000"
end_date = "2024-05-31T23:59:59.999"

# Topic name
topic = 'nyc-traffic-data-may-2024'

# Pagination settings
batch_size = 100000
offset = 0
total_count = 0

# Fetch traffic data in batches
headers = {
    'X-App-Token': api_token
}

try:
    while True:
        # Fetch a batch of data
        params = {
            '$where': f"data_as_of >= '{start_date}' AND data_as_of <= '{end_date}'",
            '$limit': batch_size,
            '$offset': offset
        }
        response = requests.get(api_url, headers=headers, params=params)
        traffic_data = response.json()

        # Break if no more data is returned
        if not traffic_data:
            print("No more data to fetch.")
            break
        
        # Publish to Kafka
        for data in traffic_data:
            # Add timestamp for real-time tracking
            data['ingestion_timestamp'] = time.time()
            
            # Convert the data to JSON string and produce to Kafka
            p.produce(topic, json.dumps(data).encode('utf-8'), callback=lambda err, msg:
                      print(f"Delivered to {msg.topic()} [{msg.partition()}]" if err is None else f"Failed delivery: {err}"))
            total_count += 1
        
        # Flush the producer to avoid buffer overflow
        p.flush()
        
        # Move to the next batch
        offset += batch_size
        print(f"Published {total_count} messages so far...")

finally:
    # Ensure all messages are sent
    p.flush()
    print(f"Published {total_count} traffic records to topic: {topic}")
