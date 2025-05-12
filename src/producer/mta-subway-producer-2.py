#!/usr/bin/env python3
import os
import json
from datetime import datetime, timezone

from sodapy import Socrata
from confluent_kafka import Producer

# ─────────────────────────── 1.  API  &  Kafka  setup ───────────────────────────
client = Socrata("data.ny.gov", "bzKl9z4AM7HbMvrLkmYenPujd")

producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'csv-json-producer',
    'queue.buffering.max.messages': 10_000_00,
    'queue.buffering.max.kbytes'  : 4_194_304,
    'batch.num.messages'          : 100_000,
    'message.timeout.ms'          : 180_000,
    'compression.type'            : 'gzip',
    'linger.ms'                   : 2_000,
    'acks'                        : 'all',
    'max.in.flight.requests.per.connection': 5
}
producer = Producer(producer_config)

# ─────────────────────────── 2.  Parameters ─────────────────────────────────────
start_date = "2024-05-01T00:00:00.000"
end_date   = "2024-05-31T23:59:59.999"
dataset_id = "wujg-7c2s"
topic      = "mta-subway-data-may-2024"

batch_size = 100_000
offset     = 0
total_sent = 0
cumulative_ridership = 0.0     # running sum (optional)

# ─────────────────────────── 3.  Helper: enrich a row ───────────────────────────
def enrich(row: dict) -> dict:
    """
    Add date parts, peak-hour flag, transfer ratio, and running total ridership.
    Leaves all original keys untouched.
    """
    global cumulative_ridership

    # Parse timestamp (it is in ISO-8601 with Z suffix → UTC)
    ts = datetime.fromisoformat(row["transit_timestamp"].replace("Z", "+00:00"))
    row["year"]  = ts.year
    row["month"] = ts.month
    row["day"]   = ts.day
    row["hour"]  = ts.hour

    # Peak-hour flag
    row["peak_hour"] = 1 if (7 <= ts.hour <= 9) or (16 <= ts.hour <= 19) else 0

    # Safe numeric parsing
    ridership = float(row.get("ridership", 0) or 0)
    transfers = float(row.get("transfers", 0) or 0)

    # Transfer ratio (guards against 0 or negative)
    row["transfer_ratio"] = (
        0.0 if ridership <= 0 or transfers <= 0
        else transfers / ridership
    )

    # Optional running total
    cumulative_ridership += ridership
    row["cumulative_ridership"] = cumulative_ridership

    return row

# ─────────────────────────── 4.  Main loop ──────────────────────────────────────
try:
    while True:
        page = client.get(
            dataset_id,
            where=f"transit_timestamp >= '{start_date}' AND transit_timestamp <= '{end_date}'",
            limit=batch_size,
            offset=offset
        )
        if not page:                       # no more rows
            print("Finished fetching.")
            break

        for raw_row in page:
            enriched = enrich(raw_row)

            producer.produce(
                topic       = topic,
                value       = json.dumps(enriched).encode("utf-8"),
                on_delivery = lambda err, msg: (
                    None if err is None else 
                    print(f"Delivery failed: {err}")
                )
            )
            total_sent += 1

        producer.flush()                   # force delivery for each batch
        offset += batch_size
        print(f"✓ Published {total_sent:,} messages so far…")

finally:
    producer.flush()
    print(f"Done. Total messages published to {topic}: {total_sent:,}")
