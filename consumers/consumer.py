from confluent_kafka import Consumer, KafkaError
import json
import socket

# -----------------------------
# Kafka Consumer Configuration
# -----------------------------
conf = {
    # External ports for MacBook-to-Docker communication
    'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9096',
    'group.id': 'taxi-consumer-group-v1', 
    'auto.offset.reset': 'earliest',      
    'client.id': socket.gethostname(),
    'enable.auto.commit': True            
}

consumer = Consumer(conf)
topic = 'taxi.raw'
consumer.subscribe([topic])

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
                print(f"Consumer error: {msg.error()}")
                break

        # -----------------------------
        # Process the Message
        # -----------------------------
        try:
            raw_value = msg.value().decode('utf-8')
            data = json.loads(raw_value)
            
            partition = msg.partition()
            borough = data.get('pickup_borough_id', 'Unknown')
            
            # Print partition info to verify the 6-partition distribution
            print(f"Received [Part {partition}] Borough: {borough} | Data: {data}")

        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
