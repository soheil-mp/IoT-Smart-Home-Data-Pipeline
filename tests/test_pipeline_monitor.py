import pytest
from datetime import datetime
from src.monitoring.pipeline_monitor import PipelineMonitor

@pytest.fixture
def db_params():
    """Database connection parameters for testing."""
    return {
        "dbname": "iot_db",
        "user": "iot_user",
        "password": "iot_password",
        "host": "localhost",
        "port": "5432"
    }

@pytest.fixture
def monitor(db_params):
    """Create a pipeline monitor instance for testing."""
    return PipelineMonitor(db_params)

def test_validate_reading(monitor):
    """Test reading validation."""
    # Valid temperature reading
    temp_reading = {
        'sensor_id': 'temp_sensor_1',
        'value': 23.5,
        'timestamp': datetime.now(),
        'location': 'room_1'
    }
    is_valid, _ = monitor.validate_reading(temp_reading)
    assert is_valid is True
    
    # Valid humidity reading
    humidity_reading = {
        'sensor_id': 'humidity_sensor_1',
        'value': 45.0,
        'timestamp': datetime.now(),
        'location': 'room_1'
    }
    is_valid, _ = monitor.validate_reading(humidity_reading)
    assert is_valid is True
    
    # Valid motion reading
    motion_reading = {
        'sensor_id': 'motion_sensor_1',
        'value': True,
        'timestamp': datetime.now(),
        'location': 'room_1'
    }
    is_valid, _ = monitor.validate_reading(motion_reading)
    assert is_valid is True

def test_invalid_readings(monitor):
    """Test validation of invalid readings."""
    # Missing required field
    invalid_reading = {
        'sensor_id': 'temp_sensor_1',
        'value': 23.5
    }
    is_valid, error = monitor.validate_reading(invalid_reading)
    assert is_valid is False
    assert error == "Missing required fields"
    
    # Invalid value type
    invalid_type = {
        'sensor_id': 'temp_sensor_1',
        'value': "not a number",
        'timestamp': datetime.now(),
        'location': 'room_1'
    }
    is_valid, error = monitor.validate_reading(invalid_type)
    assert is_valid is False
    assert "Invalid value type" in error
    
    # Out of range value
    out_of_range = {
        'sensor_id': 'temp_sensor_1',
        'value': 100.0,
        'timestamp': datetime.now(),
        'location': 'room_1'
    }
    is_valid, error = monitor.validate_reading(out_of_range)
    assert is_valid is False
    assert "Value out of range" in error

def test_record_batch_metrics(monitor):
    """Test recording of batch processing metrics."""
    monitor.record_batch_metrics(
        batch_size=10,
        processing_time=0.5,
        error_count=1
    )
    
    # Check metrics were recorded
    metrics = monitor.get_current_metrics()
    assert metrics['total_readings'] > 0
    assert metrics['error_rate'] >= 0
    assert metrics['avg_processing_time'] > 0

def test_log_metrics(monitor):
    """Test metrics logging."""
    # Record some metrics first
    monitor.record_batch_metrics(
        batch_size=10,
        processing_time=0.5,
        error_count=1
    )
    
    # Log metrics
    monitor.log_metrics()
    
    # Check metrics were reset
    metrics = monitor.get_current_metrics()
    assert metrics['batch_count'] == 0
    assert metrics['total_readings'] == 0
    assert metrics['error_count'] == 0

def test_quality_metrics(monitor):
    """Test quality metrics tracking."""
    # Process some readings
    valid_reading = {
        'sensor_id': 'temp_sensor_1',
        'value': 23.5,
        'timestamp': datetime.now(),
        'location': 'room_1'
    }
    monitor.validate_reading(valid_reading)
    
    # Check quality metrics
    quality = monitor.get_quality_metrics()
    assert 'temperature' in quality
    assert quality['temperature'].total_readings > 0

def test_cleanup(monitor):
    """Test cleanup of monitoring resources."""
    monitor.close()
    assert monitor.conn.closed 