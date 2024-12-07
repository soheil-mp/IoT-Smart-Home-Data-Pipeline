import pytest
from datetime import datetime
from src.processors.data_processor import DataProcessor
from src.simulator.sensor_simulator import SensorSimulator

@pytest.fixture
def processor():
    """Create a data processor instance for testing."""
    return DataProcessor()

@pytest.fixture
def sample_readings():
    """Generate sample sensor readings for testing."""
    simulator = SensorSimulator(num_sensors=2)
    return simulator.generate_batch()

def test_process_readings(processor, sample_readings):
    """Test processing of sensor readings."""
    processed = processor.process_readings(sample_readings)
    assert processed == len(sample_readings)

def test_validate_reading(processor):
    """Test reading validation."""
    # Valid reading
    valid_reading = {
        'sensor_id': 'temp_sensor_1',
        'timestamp': datetime.now(),
        'value': 23.5,
        'location': 'room_1'
    }
    assert processor.validate_reading(valid_reading)[0] is True
    
    # Invalid reading (missing field)
    invalid_reading = {
        'sensor_id': 'temp_sensor_1',
        'value': 23.5
    }
    assert processor.validate_reading(invalid_reading)[0] is False
    
    # Invalid reading (out of range)
    out_of_range = {
        'sensor_id': 'temp_sensor_1',
        'timestamp': datetime.now(),
        'value': 100.0,
        'location': 'room_1'
    }
    assert processor.validate_reading(out_of_range)[0] is False

def test_process_temperature(processor):
    """Test temperature reading processing."""
    reading = {
        'sensor_id': 'temp_sensor_1',
        'timestamp': datetime.now(),
        'value': 23.5,
        'location': 'room_1'
    }
    result = processor.process_reading(reading)
    assert result is True

def test_process_humidity(processor):
    """Test humidity reading processing."""
    reading = {
        'sensor_id': 'humidity_sensor_1',
        'timestamp': datetime.now(),
        'value': 45.0,
        'location': 'room_1'
    }
    result = processor.process_reading(reading)
    assert result is True

def test_process_motion(processor):
    """Test motion event processing."""
    reading = {
        'sensor_id': 'motion_sensor_1',
        'timestamp': datetime.now(),
        'value': True,
        'location': 'room_1'
    }
    result = processor.process_reading(reading)
    assert result is True

def test_batch_processing(processor):
    """Test batch processing with mixed readings."""
    simulator = SensorSimulator(num_sensors=3)
    readings = simulator.generate_batch()
    
    # Process batch
    processed = processor.process_readings(readings)
    assert processed == len(readings)
    
    # Test with empty batch
    assert processor.process_readings([]) == 0
    
    # Test with None
    assert processor.process_readings(None) == 0

def test_error_handling(processor):
    """Test error handling in processing."""
    # Invalid sensor type
    invalid_reading = {
        'sensor_id': 'unknown_sensor_1',
        'timestamp': datetime.now(),
        'value': 23.5,
        'location': 'room_1'
    }
    result = processor.process_reading(invalid_reading)
    assert result is False 