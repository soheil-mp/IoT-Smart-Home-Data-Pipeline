import logging
import psycopg2
from datetime import datetime
from typing import Dict, List, Any
import os
from dotenv import load_dotenv
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        """Initialize the data processor with database connection."""
        # Load .env file from project root
        env_path = Path(__file__).resolve().parents[2] / '.env'
        logger.info(f"Looking for .env file at: {env_path}")
        if env_path.exists():
            logger.info(".env file found")
            load_dotenv(dotenv_path=env_path)
        else:
            logger.warning(".env file not found!")
        
        # Force localhost for development
        self.db_params = {
            "dbname": "iot_db",
            "user": "iot_user",
            "password": "iot_password",
            "host": "localhost",  # Explicitly set to localhost
            "port": "5432"
        }
        
        # Log connection parameters (without password)
        logger.info("Database configuration:")
        logger.info(f"Host: {self.db_params['host']}")
        logger.info(f"Port: {self.db_params['port']}")
        logger.info(f"Database: {self.db_params['dbname']}")
        logger.info(f"User: {self.db_params['user']}")
        
        self.conn = None
        self.connect()
        logger.info("Data processor initialized")

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.conn.autocommit = False  # We'll manage transactions manually
            logger.info("Successfully connected to the database")
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            raise

    def ensure_connection(self):
        """Ensure database connection is active."""
        try:
            # Try a simple query to test connection
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            logger.warning("Database connection lost, reconnecting...")
            self.connect()

    def process_temperature_reading(self, reading: Dict[str, Any]):
        """Process and store temperature reading."""
        self.ensure_connection()
        with self.conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO temperature_readings (sensor_id, timestamp, value)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (reading["sensor_id"], reading["timestamp"], reading["value"]))
                reading_id = cur.fetchone()[0]
                self.conn.commit()
                logger.debug(f"Stored temperature reading {reading_id}")
                return reading_id
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Error storing temperature reading: {e}")
                raise

    def process_humidity_reading(self, reading: Dict[str, Any]):
        """Process and store humidity reading."""
        self.ensure_connection()
        with self.conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO humidity_readings (sensor_id, timestamp, value)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (reading["sensor_id"], reading["timestamp"], reading["value"]))
                reading_id = cur.fetchone()[0]
                self.conn.commit()
                logger.debug(f"Stored humidity reading {reading_id}")
                return reading_id
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Error storing humidity reading: {e}")
                raise

    def process_motion_event(self, reading: Dict[str, Any]):
        """Process and store motion event."""
        self.ensure_connection()
        with self.conn.cursor() as cur:
            try:
                cur.execute("""
                    INSERT INTO motion_events (sensor_id, timestamp, detected)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, (reading["sensor_id"], reading["timestamp"], reading["value"]))
                event_id = cur.fetchone()[0]
                self.conn.commit()
                logger.debug(f"Stored motion event {event_id}")
                return event_id
            except Exception as e:
                self.conn.rollback()
                logger.error(f"Error storing motion event: {e}")
                raise

    def process_readings(self, readings: List[Dict[str, Any]]):
        """Process a batch of sensor readings."""
        processed_count = 0
        for reading in readings:
            try:
                if "temp_sensor" in reading["sensor_id"]:
                    self.process_temperature_reading(reading)
                elif "humidity_sensor" in reading["sensor_id"]:
                    self.process_humidity_reading(reading)
                elif "motion_sensor" in reading["sensor_id"]:
                    self.process_motion_event(reading)
                processed_count += 1
            except Exception as e:
                logger.error(f"Error processing reading from {reading['sensor_id']}: {e}")
                continue
        
        logger.info(f"Processed {processed_count} out of {len(readings)} readings")
        return processed_count

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    # Example usage
    from src.simulator.sensor_simulator import SensorSimulator
    
    simulator = SensorSimulator(num_sensors=3)
    processor = DataProcessor()
    
    try:
        # Generate and process a batch of readings
        readings = simulator.generate_batch()
        processed = processor.process_readings(readings)
        print(f"Successfully processed {processed} readings")
    finally:
        processor.close() 