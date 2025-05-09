from confluent_kafka import Producer
import json
import requests

# Fetch data from the MTA subway entrance/exit API
response = requests.get('https://data.ny.gov/resource/i9wp-a4ja.json')
subway_data = response.json()

# Create a Kafka producer
# Using the port exposed in your docker-compose file
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Topic name
topic = 'mta-subway-data'

# Process and publish each record
count = 0
for data in subway_data:
    # You can transform data here if needed
    # For example:
    # - Add a timestamp
    # - Restructure fields
    # - Filter specific stations
    
    # Convert the data to JSON string and produce to Kafka
    print(f'Producing message {count}: {data["station_name"] if "station_name" in data else "Unknown"}')
    p.produce(topic, json.dumps(data))
    count += 1
    
    # Optional: Call poll to improve performance
    p.poll(0)

# Ensure all messages are sent
p.flush()
print(f"Published {count} messages to topic: {topic}")
