import pytest
from datetime import datetime
from src.simulator.sensor_simulator import SensorSimulator

def test_sensor_initialization():
    """Test sensor simulator initialization."""
    num_sensors = 3
    simulator = SensorSimulator(num_sensors=num_sensors)
    
    # Check if correct number of sensors are created
    expected_total = num_sensors * 3  # temperature, humidity, and motion sensors
    assert len(simulator.sensors) == expected_total
    
    # Check sensor types
    sensor_types = {'temperature', 'humidity', 'motion'}
    actual_types = {sensor['type'] for sensor in simulator.sensors.values()}
    assert actual_types == sensor_types

def test_temperature_reading():
    """Test temperature reading generation."""
    simulator = SensorSimulator(num_sensors=1)
    sensor_id = 'temp_sensor_1'
    
    # Generate multiple readings to test bounds
    readings = [simulator.generate_temperature_reading(sensor_id) for _ in range(100)]
    
    # Check bounds (15-30°C)
    assert all(15.0 <= reading <= 30.0 for reading in readings)
    # Check precision (2 decimal places)
    assert all(len(str(reading).split('.')[-1]) <= 2 for reading in readings)

def test_humidity_reading():
    """Test humidity reading generation."""
    simulator = SensorSimulator(num_sensors=1)
    sensor_id = 'humidity_sensor_1'
    
    # Generate multiple readings to test bounds
    readings = [simulator.generate_humidity_reading(sensor_id) for _ in range(100)]
    
    # Check bounds (30-70%)
    assert all(30.0 <= reading <= 70.0 for reading in readings)
    # Check precision (2 decimal places)
    assert all(len(str(reading).split('.')[-1]) <= 2 for reading in readings)

def test_motion_event():
    """Test motion event generation."""
    simulator = SensorSimulator(num_sensors=1)
    sensor_id = 'motion_sensor_1'
    
    # Generate multiple events
    events = [simulator.generate_motion_event(sensor_id) for _ in range(100)]
    
    # Check that we get both True and False values
    assert True in events
    assert False in events

def test_reading_format():
    """Test the format of generated readings."""
    simulator = SensorSimulator(num_sensors=1)
    readings = simulator.generate_batch()
    
    for reading in readings:
        # Check required fields
        assert 'sensor_id' in reading
        assert 'timestamp' in reading
        assert 'value' in reading
        assert 'location' in reading
        
        # Check timestamp is current
        timestamp = reading['timestamp']
        assert isinstance(timestamp, datetime)
        time_diff = datetime.now() - timestamp
        assert time_diff.total_seconds() < 1  # Reading should be very recent

def test_batch_generation():
    """Test batch reading generation."""
    num_sensors = 3
    simulator = SensorSimulator(num_sensors=num_sensors)
    
    # Test full batch
    full_batch = simulator.generate_batch()
    assert len(full_batch) == num_sensors * 3  # All sensors
    
    # Test partial batch
    batch_size = 4
    partial_batch = simulator.generate_batch(batch_size=batch_size)
    assert len(partial_batch) == batch_size 