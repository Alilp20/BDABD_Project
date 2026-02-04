from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient
from pymongo.errors import AutoReconnect
import json
import socket
import time

# -----------------------------
# MongoDB Connection
# -----------------------------
# Using [::1] (IPv6) to match the Docker listener found in lsof.
# directConnection=True ensures the client stays connected to the mongos router.
MONGO_URI = "mongodb://[::1]:27017/?directConnection=true&serverSelectionTimeoutMS=5000"

mongo_client = MongoClient(MONGO_URI)
db = mongo_client["taxi_db"]
collection = db["taxi_events"]

# -----------------------------
# Kafka Consumer Configuration
# -----------------------------
conf = {
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'group.id': 'mongo-consumer-group',
    'auto.offset.reset': 'earliest',
    'client.id': socket.gethostname(),
    'enable.auto.commit': True
}

consumer = Consumer(conf)
topic = 'taxi.raw'
consumer.subscribe([topic])

print(f"MongoDB Consumer connected to {MONGO_URI}")
print(f"Listening to topic '{topic}'... (Press Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Kafka error: {msg.error()}")
                break

        try:
            data = json.loads(msg.value().decode('utf-8'))

            # Compute metrics
            if data.get('trip_duration_min') and data.get('trip_distance'):
                duration_hours = data['trip_duration_min'] / 60
                if duration_hours > 0:
                    data['speed_kmh'] = round(data['trip_distance'] / duration_hours, 2)
                
                if data['trip_distance'] > 0:
                    data['price_per_km'] = round(data.get('fare_amount', 0) / data['trip_distance'], 2)

            # -----------------------------
            # Insert into MongoDB with Retry
            # -----------------------------
            try:
                collection.insert_one(data)
                print(f"Inserted trip_id {data.get('trip_id')}")
            except AutoReconnect:
                print("Connection lost. Retrying in 2 seconds...")
                time.sleep(2)
                collection.insert_one(data)

        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

except KeyboardInterrupt:
    print("\nStopping MongoDB consumer...")
finally:
    consumer.close()
