from sodapy import Socrata
from confluent_kafka import Producer
import json

# Initialize the Socrata client
client = Socrata("data.ny.gov", "bzKl9z4AM7HbMvrLkmYenPujd")

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'csv-json-producer',
    'queue.buffering.max.messages': 10000000,
    'queue.buffering.max.kbytes': 4194304,
    'batch.num.messages': 100000,
    'message.timeout.ms': 180000,
    'compression.type': 'gzip',
    'linger.ms': 2000,
    'acks': 'all',
    'max.in.flight.requests.per.connection': 5
}

# Create a Kafka producer
p = Producer(producer_config)

# Define the date range for May 2024
start_date = "2024-05-01T00:00:00.000"
end_date = "2024-05-31T23:59:59.999"

# Topic name
topic = 'mta-subway-data-may-2024'

# Pagination settings
batch_size = 100000
offset = 0
total_count = 0

try:
    while True:
        # fetch a page
        subway_data = client.get(
            "wujg-7c2s",
            where=f"transit_timestamp >= '{start_date}' AND transit_timestamp <= '{end_date}'",
            limit=batch_size,
            offset=offset
        )

        if not subway_data:          # no more rows
            print("No more data to fetch.")
            break

        # ---------- 4  publish unchanged rows ----------
        for row in subway_data:
            p.produce(
                topic,
                json.dumps(row).encode("utf-8"),
                callback=lambda err, msg: (
                    print(f"Delivered to {msg.topic()} [{msg.partition()}]")
                    if err is None else
                    print(f"Failed delivery: {err}")
                )
            )
            total_count += 1

        p.flush()                    # flush each page
        offset += batch_size
        print(f"Published {total_count} messages so far...")

finally:
    # Ensure all messages are sent
    p.flush()
    print(f"Published {total_count} messages to topic: {topic}")
