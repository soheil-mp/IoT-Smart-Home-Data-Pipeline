import os
from typing import Dict, Any

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    'topics': {
        'temperature': 'temperature_data',
        'humidity': 'humidity_data',
        'motion': 'motion_data'
    }
}

# PostgreSQL Configuration
POSTGRES_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB', 'iot_db'),
    'user': os.getenv('POSTGRES_USER', 'iot_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'iot_password')
}

# InfluxDB Configuration
INFLUXDB_CONFIG = {
    'url': os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
    'token': os.getenv('INFLUXDB_TOKEN', ''),
    'org': os.getenv('INFLUXDB_ORG', 'iot_org'),
    'bucket': os.getenv('INFLUXDB_BUCKET', 'iot_bucket')
}

# Flink Configuration
FLINK_CONFIG = {
    'job_name': 'IoT Sensor Data Processing',
    'parallelism': int(os.getenv('FLINK_PARALLELISM', '2')),
    'checkpoint_interval': 60000,  # milliseconds
    'state_backend': 'filesystem',
    'state_backend_path': os.getenv('FLINK_STATE_PATH', '/tmp/flink-checkpoints')
}

# Sensor Configuration
SENSOR_CONFIG = {
    'num_sensors': int(os.getenv('NUM_SENSORS', '5')),
    'sampling_interval': float(os.getenv('SAMPLING_INTERVAL', '1.0')),  # seconds
    'temperature_range': (18.0, 30.0),  # celsius
    'humidity_range': (30.0, 70.0),  # percent
    'motion_probability': 0.2  # 20% chance of motion detection
}

# Alert Thresholds
ALERT_THRESHOLDS = {
    'temperature_high': float(os.getenv('TEMP_HIGH_THRESHOLD', '28.0')),
    'temperature_low': float(os.getenv('TEMP_LOW_THRESHOLD', '18.0')),
    'humidity_high': float(os.getenv('HUMIDITY_HIGH_THRESHOLD', '65.0')),
    'humidity_low': float(os.getenv('HUMIDITY_LOW_THRESHOLD', '35.0'))
}

# Logging Configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'file': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.FileHandler',
            'filename': 'iot_pipeline.log',
            'mode': 'a'
        }
    },
    'loggers': {
        '': {
            'handlers': ['default', 'file'],
            'level': 'INFO',
            'propagate': True
        }
    }
}

def get_config() -> Dict[str, Any]:
    """Return all configuration settings."""
    return {
        'kafka': KAFKA_CONFIG,
        'postgres': POSTGRES_CONFIG,
        'influxdb': INFLUXDB_CONFIG,
        'flink': FLINK_CONFIG,
        'sensor': SENSOR_CONFIG,
        'alerts': ALERT_THRESHOLDS,
        'logging': LOGGING_CONFIG
    } 