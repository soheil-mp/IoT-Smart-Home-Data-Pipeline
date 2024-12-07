import random
import time
from datetime import datetime
import logging
from typing import Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SensorSimulator:
    def __init__(self, num_sensors: int = 5):
        """Initialize the sensor simulator with a specified number of sensors."""
        self.sensors: Dict[str, Dict] = {
            f"temp_sensor_{i}": {"type": "temperature", "location": f"room_{i}", "last_value": 20.0}
            for i in range(1, num_sensors + 1)
        }
        self.sensors.update({
            f"humidity_sensor_{i}": {"type": "humidity", "location": f"room_{i}", "last_value": 50.0}
            for i in range(1, num_sensors + 1)
        })
        self.sensors.update({
            f"motion_sensor_{i}": {"type": "motion", "location": f"room_{i}", "last_value": False}
            for i in range(1, num_sensors + 1)
        })
        logger.info(f"Initialized {len(self.sensors)} sensors")

    def generate_temperature_reading(self, sensor_id: str, base_temp: float = 20.0) -> float:
        """Generate a realistic temperature reading with some random variation."""
        current = self.sensors[sensor_id]["last_value"]
        # Add some random variation (-0.5 to +0.5) with momentum
        variation = random.uniform(-0.5, 0.5)
        new_temp = current + variation
        # Keep temperature within realistic bounds (15-30°C)
        new_temp = max(15.0, min(30.0, new_temp))
        self.sensors[sensor_id]["last_value"] = new_temp
        return round(new_temp, 2)

    def generate_humidity_reading(self, sensor_id: str, base_humidity: float = 50.0) -> float:
        """Generate a realistic humidity reading with some random variation."""
        current = self.sensors[sensor_id]["last_value"]
        # Add some random variation (-2 to +2) with momentum
        variation = random.uniform(-2.0, 2.0)
        new_humidity = current + variation
        # Keep humidity within realistic bounds (30-70%)
        new_humidity = max(30.0, min(70.0, new_humidity))
        self.sensors[sensor_id]["last_value"] = new_humidity
        return round(new_humidity, 2)

    def generate_motion_event(self, sensor_id: str) -> bool:
        """Generate a motion event with some probability."""
        # 10% chance of motion detection if previously no motion
        # 70% chance of continued motion if previously detected
        prev_state = self.sensors[sensor_id]["last_value"]
        if prev_state:
            new_state = random.random() < 0.7  # 70% chance to continue
        else:
            new_state = random.random() < 0.1  # 10% chance to detect
        self.sensors[sensor_id]["last_value"] = new_state
        return new_state

    def generate_reading(self, sensor_id: str) -> Dict:
        """Generate a reading for a specific sensor."""
        sensor = self.sensors[sensor_id]
        timestamp = datetime.now()
        
        reading = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "location": sensor["location"]
        }

        if sensor["type"] == "temperature":
            reading["value"] = self.generate_temperature_reading(sensor_id)
            reading["unit"] = "celsius"
        elif sensor["type"] == "humidity":
            reading["value"] = self.generate_humidity_reading(sensor_id)
            reading["unit"] = "percent"
        else:  # motion
            reading["value"] = self.generate_motion_event(sensor_id)
            reading["unit"] = "boolean"

        return reading

    def generate_batch(self, batch_size: Optional[int] = None) -> List[Dict]:
        """Generate readings for all or a subset of sensors."""
        if batch_size is None:
            sensor_ids = list(self.sensors.keys())
        else:
            sensor_ids = random.sample(list(self.sensors.keys()), min(batch_size, len(self.sensors)))
        
        readings = []
        for sensor_id in sensor_ids:
            readings.append(self.generate_reading(sensor_id))
        
        return readings

if __name__ == "__main__":
    # Example usage
    simulator = SensorSimulator(num_sensors=3)
    readings = simulator.generate_batch()
    for reading in readings:
        print(f"Sensor: {reading['sensor_id']}, Value: {reading['value']} {reading['unit']}") 