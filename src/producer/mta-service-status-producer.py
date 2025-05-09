from confluent_kafka import Producer
import json
import requests
import time

# Fetch data from the MTA service alerts API
response = requests.get('https://data.ny.gov/resource/7kct-peq7.json')
alerts_data = response.json()

# Create a Kafka producer
p = Producer({'bootstrap.servers': 'localhost:9092'})

# Topic name
topic = 'mta-service-alerts'

# Process and publish each record
count = 0
for alert in alerts_data:
    # Add timestamp for ingestion tracking
    alert['ingestion_timestamp'] = time.time()
    
    # Convert the data to JSON string and produce to Kafka
    alert_info = f"{alert.get('planned_work_heading', alert.get('alert_heading', 'Unknown Alert'))}"
    print(f'Producing alert {count}: {alert_info}')
    p.produce(topic, json.dumps(alert))
    count += 1
    
    # Optional: Call poll to improve performance
    p.poll(0)

# Ensure all messages are sent
p.flush()
print(f"Published {count} service alerts to topic: {topic}")
