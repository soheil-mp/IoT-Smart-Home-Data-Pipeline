import time
import logging
import signal
import sys
from simulator.sensor_simulator import SensorSimulator
from processors.data_processor import DataProcessor
from processors.analytics_processor import AnalyticsProcessor
from monitoring.pipeline_monitor import PipelineMonitor

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class IoTDataPipeline:
    def __init__(self, num_sensors: int = 5, interval: float = 1.0):
        """Initialize the IoT data pipeline."""
        # Database configuration
        self.db_params = {
            "dbname": "iot_db",
            "user": "iot_user",
            "password": "iot_password",
            "host": "localhost",
            "port": "5432"
        }
        
        # Initialize components
        self.simulator = SensorSimulator(num_sensors=num_sensors)
        self.processor = DataProcessor()
        self.analytics = AnalyticsProcessor(self.db_params)
        self.monitor = PipelineMonitor(self.db_params)
        
        self.interval = interval
        self.running = False
        logger.info(f"IoT Data Pipeline initialized with {num_sensors} sensors")
        
        # Set up signal handlers
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

    def handle_shutdown(self, signum, frame):
        """Handle shutdown signals gracefully."""
        logger.info("Shutdown signal received, stopping pipeline...")
        self.running = False

    def run(self):
        """Run the data pipeline."""
        self.running = True
        total_readings = 0
        start_time = time.time()
        last_analytics_time = start_time
        analytics_interval = 300  # Run analytics every 5 minutes

        try:
            logger.info("Starting IoT data pipeline...")
            while self.running:
                batch_start_time = time.time()
                
                # Generate and validate readings
                readings = self.simulator.generate_batch()
                valid_readings = []
                for reading in readings:
                    is_valid, error = self.monitor.validate_reading(reading)
                    if is_valid:
                        valid_readings.append(reading)
                    else:
                        logger.warning(f"Invalid reading from {reading['sensor_id']}: {error}")
                
                # Process valid readings
                processed = self.processor.process_readings(valid_readings)
                total_readings += processed
                
                # Record batch metrics
                processing_time = time.time() - batch_start_time
                self.monitor.record_batch_metrics(
                    batch_size=len(readings),
                    processing_time=processing_time,
                    error_count=len(readings) - processed
                )
                
                # Run analytics periodically
                current_time = time.time()
                if current_time - last_analytics_time >= analytics_interval:
                    logger.info("Running analytics processing...")
                    self.analytics.process_analytics()
                    last_analytics_time = current_time
                
                # Log monitoring metrics periodically
                if total_readings % 100 == 0:
                    self.monitor.log_metrics()
                
                # Calculate and log statistics
                elapsed_time = time.time() - start_time
                readings_per_second = total_readings / elapsed_time
                logger.info(f"Processing rate: {readings_per_second:.2f} readings/second")

                # Wait for next interval
                time.sleep(self.interval)

        except Exception as e:
            logger.error(f"Error in pipeline: {e}")
            raise
        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        self.processor.close()
        self.analytics.close()
        self.monitor.close()
        logger.info("Pipeline shutdown complete")

def main():
    """Main entry point for the IoT data pipeline."""
    try:
        pipeline = IoTDataPipeline(num_sensors=5, interval=1.0)
        pipeline.run()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 