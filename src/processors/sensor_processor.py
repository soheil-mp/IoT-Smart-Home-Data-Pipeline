"""IoT Sensor Data Processor.

This module consumes sensor data from Kafka topics, processes it,
and stores it in PostgreSQL and InfluxDB for analysis and visualization.
"""
import json
import os
from datetime import datetime
from typing import Dict, Any

from confluent_kafka import Consumer, KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

# Load environment variables from the root directory
root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(root_dir, '.env')
print(f"\nLoading environment from: {env_path}")
if os.path.exists(env_path):
    with open(env_path, 'r') as f:
        print("Environment file contents:")
        print(f.read())
load_dotenv(env_path, override=True)

class SensorProcessor:
    def __init__(self):
        print("\nInitializing sensor processor...")
        
        # Kafka configuration
        kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        print(f"Kafka host: {kafka_host}")
        self.consumer = Consumer({
            'bootstrap.servers': kafka_host,
            'group.id': 'sensor_processor_group',
            'auto.offset.reset': 'earliest'
        })
        
        # Subscribe to topics
        self.topics = [
            os.getenv('KAFKA_TOPIC_TEMPERATURE', 'temperature_data'),
            os.getenv('KAFKA_TOPIC_HUMIDITY', 'humidity_data'),
            os.getenv('KAFKA_TOPIC_MOTION', 'motion_data')
        ]
        print(f"Subscribing to topics: {self.topics}")
        self.consumer.subscribe(self.topics)
        
        # PostgreSQL connection
        # Force localhost for local development
        pg_host = 'localhost'  # Force localhost
        pg_port = os.getenv('POSTGRES_PORT', '5432')
        pg_db = os.getenv('POSTGRES_DB', 'iot_db')
        pg_user = os.getenv('POSTGRES_USER', 'iot_user')
        pg_password = os.getenv('POSTGRES_PASSWORD', 'iot_password')
        
        print(f"PostgreSQL connection details:")
        print(f"Host: {pg_host}")
        print(f"Port: {pg_port}")
        print(f"Database: {pg_db}")
        print(f"User: {pg_user}")
        
        self.pg_conn = psycopg2.connect(
            host=pg_host,
            port=pg_port,
            database=pg_db,
            user=pg_user,
            password=pg_password
        )
        self.pg_cursor = self.pg_conn.cursor()
        
        # InfluxDB connection
        influx_url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
        print(f"InfluxDB URL: {influx_url}")
        self.influx_client = InfluxDBClient(
            url=influx_url,
            token=os.getenv('INFLUXDB_TOKEN', 'iot-pipeline-token-2024'),
            org=os.getenv('INFLUXDB_ORG', 'iot_org')
        )
        self.influx_write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        self.influx_bucket = os.getenv('INFLUXDB_BUCKET', 'iot_bucket')
        print("Initialization complete!")

    def process_message(self, message: Dict[str, Any]) -> None:
        """Process a single sensor message."""
        try:
            # Extract data
            sensor_id = message['sensor_id']
            sensor_type = message['type']
            value = message['value']
            timestamp = datetime.fromisoformat(message['timestamp'])
            
            # Store in PostgreSQL
            if sensor_type == 'temperature':
                self.pg_cursor.execute("""
                    INSERT INTO temperature_readings (sensor_id, value, timestamp)
                    VALUES (%s, %s, %s)
                """, (sensor_id, value, timestamp))
            elif sensor_type == 'humidity':
                self.pg_cursor.execute("""
                    INSERT INTO humidity_readings (sensor_id, value, timestamp)
                    VALUES (%s, %s, %s)
                """, (sensor_id, value, timestamp))
            elif sensor_type == 'motion':
                self.pg_cursor.execute("""
                    INSERT INTO motion_events (sensor_id, detected, timestamp)
                    VALUES (%s, %s, %s)
                """, (sensor_id, value, timestamp))
            
            self.pg_conn.commit()
            
            # Store in InfluxDB
            point = Point(sensor_type) \
                .tag("sensor_id", sensor_id) \
                .field("value", value) \
                .time(timestamp)
            
            self.influx_write_api.write(
                bucket=self.influx_bucket,
                record=point
            )
            
            print(f"Processed {sensor_type} reading from {sensor_id}: {value}")
            
        except Exception as e:
            print(f"Error processing message: {str(e)}")
            self.pg_conn.rollback()

    def run(self):
        """Run the processor continuously."""
        try:
            print("Starting sensor processor...")
            print("Press Ctrl+C to stop")
            
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f"Reached end of partition {msg.partition()}")
                    else:
                        print(f"Error: {msg.error()}")
                    continue
                
                try:
                    message = json.loads(msg.value().decode('utf-8'))
                    self.process_message(message)
                except json.JSONDecodeError as e:
                    print(f"Error decoding message: {str(e)}")
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                
        except KeyboardInterrupt:
            print("\nStopping sensor processor...")
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources."""
        if self.pg_cursor:
            self.pg_cursor.close()
        if self.pg_conn:
            self.pg_conn.close()
        if self.consumer:
            self.consumer.close()
        if self.influx_write_api:
            self.influx_write_api.close()
        if self.influx_client:
            self.influx_client.close()

if __name__ == "__main__":
    processor = SensorProcessor()
    processor.run()
