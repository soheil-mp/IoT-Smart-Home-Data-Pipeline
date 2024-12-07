"""Database configuration and connection utilities."""
import os
from typing import Dict, Any

# Database connection settings
DB_CONFIG = {
    'host': 'localhost',
    'user': 'iot_user',
    'password': 'iot_password',
    'database': 'iot_db',
    'port': '5432'
}

# Table schemas
TABLE_SCHEMAS: Dict[str, str] = {
    'sensors': """
        sensor_id VARCHAR(50) PRIMARY KEY,
        type VARCHAR(20) NOT NULL,
        location VARCHAR(100),
        status VARCHAR(20) DEFAULT 'active',
        last_reading TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """,
    'temperature_readings': """
        id SERIAL,
        sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
        value DECIMAL(5,2) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """,
    'humidity_readings': """
        id SERIAL,
        sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
        value DECIMAL(5,2) NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """,
    'motion_events': """
        id SERIAL,
        sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
        detected BOOLEAN NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """,
    'sensor_analytics': """
        id SERIAL PRIMARY KEY,
        sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
        metric_name VARCHAR(50) NOT NULL,
        value DECIMAL(10,2) NOT NULL,
        window_start TIMESTAMP NOT NULL,
        window_end TIMESTAMP NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """
}

# Index definitions
INDEX_DEFINITIONS = [
    ("idx_temp_sensor_timestamp", "temperature_readings(sensor_id, timestamp)"),
    ("idx_humidity_sensor_timestamp", "humidity_readings(sensor_id, timestamp)"),
    ("idx_motion_sensor_timestamp", "motion_events(sensor_id, timestamp)"),
    ("idx_analytics_sensor_metric", "sensor_analytics(sensor_id, metric_name, window_start)")
]

# Initial sensor data
INITIAL_SENSORS = [
    ('temp_sensor_1', 'temperature', 'Living Room'),
    ('temp_sensor_2', 'temperature', 'Kitchen'),
    ('temp_sensor_3', 'temperature', 'Bedroom'),
    ('temp_sensor_4', 'temperature', 'Office'),
    ('temp_sensor_5', 'temperature', 'Bathroom'),
    ('humidity_sensor_1', 'humidity', 'Living Room'),
    ('humidity_sensor_2', 'humidity', 'Kitchen'),
    ('humidity_sensor_3', 'humidity', 'Bedroom'),
    ('humidity_sensor_4', 'humidity', 'Office'),
    ('humidity_sensor_5', 'humidity', 'Bathroom'),
    ('motion_sensor_1', 'motion', 'Living Room'),
    ('motion_sensor_2', 'motion', 'Kitchen'),
    ('motion_sensor_3', 'motion', 'Bedroom'),
    ('motion_sensor_4', 'motion', 'Office'),
    ('motion_sensor_5', 'motion', 'Bathroom')
]

# Partitioned tables
PARTITIONED_TABLES = ['temperature_readings', 'humidity_readings', 'motion_events'] 