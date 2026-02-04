from confluent_kafka import Producer
import json
import socket
import time
import os

# -----------------------------
# Kafka configuration
# -----------------------------
conf = {
    # Connect to the brokers via EXTERNAL ports
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'client.id': socket.gethostname(),  # optional but useful for debugging
}

producer = Producer(conf)

# -----------------------------
# Delivery callback
# -----------------------------
def delivery_report(err, msg):
    """
    Called once per message to report delivery status
    """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# -----------------------------
# Send single record
# -----------------------------
def send_data(topic, data, key=None):
    """
    Send one record to Kafka
    key: partitioning key (optional)
    """
    producer.poll(0)  # trigger delivery report callbacks
    producer.produce(
        topic,
        key=str(key).encode('utf-8') if key else None,
        value=json.dumps(data).encode('utf-8'),
        callback=delivery_report
    )

# -----------------------------
# Stream a JSONL file
# -----------------------------
def stream_jsonl_file(file_path, topic, key_field=None, rate=0):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    count = 0
    with open(file_path, 'r') as f:
        for line in f:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            key = record.get(key_field) if key_field else None
            
            # Send data
            producer.produce(
                topic,
                key=str(key).encode('utf-8') if key else None,
                value=json.dumps(record).encode('utf-8'),
                callback=delivery_report
            )
            
            # Serve delivery callbacks every 100 messages to maintain speed
            count += 1
            if count % 100 == 0:
                producer.poll(0)

            if rate > 0:
                time.sleep(rate)

    # BLOCK until all final messages are sent
    print("Flushing final messages...")
    producer.flush()
    print("Streaming complete.")


# -----------------------------
# Main
# -----------------------------
if __name__ == "__main__":
    # Path to your taxi events JSONL file
    file_path = '/Users/mac/Desktop/bdabd_project/data/cleaned_data/taxi_events.jsonl'
    topic = 'taxi.raw'
    key_field = 'pickup_borough_id'  # optional, can be used for partitioning
    rate = 0.001  # 1ms delay between messages (adjust if needed)

    stream_jsonl_file(file_path, topic, key_field=key_field, rate=rate)
