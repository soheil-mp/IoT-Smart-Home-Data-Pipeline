"""Shared test fixtures and configuration."""
import pytest
import psycopg2
from datetime import datetime, timedelta

@pytest.fixture(scope="session")
def db_params():
    """Database connection parameters for testing."""
    return {
        "dbname": "iot_db",
        "user": "iot_user",
        "password": "iot_password",
        "host": "localhost",
        "port": "5432"
    }

@pytest.fixture(scope="session")
def db_connection(db_params):
    """Create a database connection for testing."""
    conn = psycopg2.connect(**db_params)
    yield conn
    conn.close()

@pytest.fixture
def sample_readings():
    """Generate sample sensor readings for testing."""
    base_time = datetime.now()
    return [
        {
            'sensor_id': 'temp_sensor_1',
            'value': 23.5,
            'timestamp': base_time,
            'location': 'room_1'
        },
        {
            'sensor_id': 'humidity_sensor_1',
            'value': 45.0,
            'timestamp': base_time + timedelta(seconds=1),
            'location': 'room_1'
        },
        {
            'sensor_id': 'motion_sensor_1',
            'value': True,
            'timestamp': base_time + timedelta(seconds=2),
            'location': 'room_1'
        }
    ]

@pytest.fixture(autouse=True)
def cleanup_db(db_connection):
    """Clean up test data after each test."""
    yield
    with db_connection.cursor() as cur:
        cur.execute("""
            DELETE FROM sensor_data 
            WHERE sensor_id LIKE 'test_%'
        """)
        cur.execute("""
            DELETE FROM aggregated_metrics 
            WHERE sensor_id LIKE 'test_%'
        """)
        cur.execute("""
            DELETE FROM pipeline_metrics 
            WHERE component LIKE 'test_%'
        """)
    db_connection.commit() 