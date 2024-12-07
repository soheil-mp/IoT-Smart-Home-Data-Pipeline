import pytest
from datetime import datetime, timedelta
from src.processors.analytics_processor import AnalyticsProcessor

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
def processor(db_params):
    """Create an analytics processor instance for testing."""
    return AnalyticsProcessor(db_params)

def test_connection(processor):
    """Test database connection."""
    assert processor.conn is not None
    assert not processor.conn.closed

def test_calculate_statistics(processor):
    """Test statistics calculation."""
    # Sample data
    values = [20.5, 21.0, 22.5, 21.5, 20.0]
    timestamps = [
        datetime.now() - timedelta(minutes=i)
        for i in range(len(values))
    ]
    
    stats = processor.calculate_statistics(
        sensor_id='temp_sensor_1',
        values=values,
        timestamps=timestamps
    )
    
    assert stats.min_value == 20.0
    assert stats.max_value == 22.5
    assert 20.0 <= stats.avg_value <= 22.5
    assert stats.count == len(values)

def test_get_sensor_trends(processor):
    """Test retrieving sensor trends."""
    trends = processor.get_sensor_trends(hours=24)
    
    # Check structure
    assert 'temperature' in trends
    assert 'humidity' in trends
    assert 'motion' in trends
    
    # Check data format
    if trends['temperature']:
        temp_trend = trends['temperature'][0]
        assert 'sensor_id' in temp_trend
        assert 'hour' in temp_trend
        assert 'avg' in temp_trend
        assert 'min' in temp_trend
        assert 'max' in temp_trend

def test_process_analytics(processor):
    """Test analytics processing."""
    result = processor.process_analytics()
    assert result is True

def test_get_latest_readings(processor):
    """Test retrieving latest readings."""
    readings = processor.get_latest_readings()
    
    # Check structure
    assert 'temperature' in readings
    assert 'humidity' in readings
    assert 'motion' in readings
    
    # Check data format
    for category in readings.values():
        if category:
            reading = category[0]
            assert 'sensor_id' in reading
            assert 'value' in reading
            assert 'timestamp' in reading
            assert 'location' in reading

def test_error_handling(processor):
    """Test error handling in analytics processing."""
    # Test with invalid date range
    future_date = datetime.now() + timedelta(days=365)
    trends = processor.get_sensor_trends(
        start_time=future_date,
        end_time=future_date + timedelta(hours=1)
    )
    assert not trends['temperature']
    assert not trends['humidity']
    assert not trends['motion']

def test_cleanup(processor):
    """Test cleanup of database resources."""
    processor.close()
    assert processor.conn.closed 