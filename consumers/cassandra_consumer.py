import os
import json
import time
import logging
import uuid
import socket
from datetime import datetime
from kafka import KafkaConsumer

# Cassandra imports
from cassandra.cluster import Cluster
from cassandra.io.asyncioreactor import AsyncioConnection # Use the modern native reactor
from cassandra.policies import RoundRobinPolicy

# Setup Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CassandraConsumer")

def connect_cassandra():
    """Connects using the Asyncio reactor - stable for Python 3.12/3.13/3.14"""
    while True:
        try:
            # Connect to the exposed port on localhost
            cluster = Cluster(
                ['127.0.0.1'], 
                port=9042,
                connection_class=AsyncioConnection, # No more libev/asyncore issues
                load_balancing_policy=RoundRobinPolicy(),
                protocol_version=5
            )
            session = cluster.connect()
            logger.info("Successfully connected using Asyncio reactor.")
            return session, cluster
        except Exception as e:
            logger.warning(f"Waiting for Cassandra... ({e})")
            time.sleep(5)

def run():
    session, cluster = connect_cassandra()
    
    # 1. Setup Keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS taxi_ks 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
    """)
    session.set_keyspace('taxi_ks')

    # 2. Analytical Table Design: Partitioned by Borough, Sorted by Time
    session.execute("""
        CREATE TABLE IF NOT EXISTS trips_by_borough (
            borough text,
            pickup_datetime timestamp,
            trip_id uuid,
            fare_amount float,
            trip_distance float,
            PRIMARY KEY (borough, pickup_datetime, trip_id)
        ) WITH CLUSTERING ORDER BY (pickup_datetime DESC, trip_id ASC);
    """)

    # 3. Prepared Statement for efficiency
    prepared = session.prepare("""
        INSERT INTO trips_by_borough (borough, pickup_datetime, trip_id, fare_amount, trip_distance)
        VALUES (?, ?, ?, ?, ?)
    """)

    # 4. Kafka Consumer Setup
    logger.info("Connecting to Kafka...")
    consumer = KafkaConsumer(
        'taxi.raw',
        bootstrap_servers=['localhost:9092', 'localhost:9094', 'localhost:9096'],
        group_id='cassandra-async-group',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logger.info("🚀 Consuming and saving to Cassandra...")
    
    try:
        for message in consumer:
            data = message.value
            try:
                # Map fields from your Kafka message
                borough = data.get('borough', 'Unknown')
                
                # Parse timestamp
                raw_ts = data.get('tpep_pickup_datetime') or data.get('pickup_datetime')
                pickup_ts = datetime.strptime(raw_ts, '%Y-%m-%d %H:%M:%S')
                
                # Insert data
                session.execute(prepared, (
                    borough,
                    pickup_ts,
                    uuid.uuid4(),
                    float(data.get('fare_amount', 0.0)),
                    float(data.get('trip_distance', 0.0))
                ))
                
                logger.info(f"📥 Saved: {borough} | {raw_ts}")

            except Exception as e:
                logger.error(f"❌ Processing Error: {e}")

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        consumer.close()
        cluster.shutdown()

if __name__ == "__main__":
    # Ensure driver uses pure python path if extensions are still failing
    os.environ['CASS_DRIVER_NO_EXTENSIONS'] = '1'
    run()
