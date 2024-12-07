import logging
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import time
from collections import deque
from dataclasses import dataclass
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class DataQualityMetrics:
    """Data quality metrics for sensor readings."""
    total_readings: int = 0
    invalid_readings: int = 0
    missing_values: int = 0
    out_of_range_values: int = 0

@dataclass
class PerformanceMetrics:
    """Performance metrics for the pipeline."""
    processing_time: float = 0.0
    readings_per_second: float = 0.0
    error_count: int = 0
    batch_size: int = 0

class PipelineMonitor:
    def __init__(self, db_params: Dict[str, str], window_size: int = 100):
        """Initialize the pipeline monitor."""
        self.db_params = db_params
        self.window_size = window_size
        self.conn = None
        self.start_time = time.time()
        
        # Monitoring windows
        self.processing_times = deque(maxlen=window_size)
        self.error_counts = deque(maxlen=window_size)
        self.batch_sizes = deque(maxlen=window_size)
        
        # Data quality metrics
        self.quality_metrics = {
            'temperature': DataQualityMetrics(),
            'humidity': DataQualityMetrics(),
            'motion': DataQualityMetrics()
        }
        
        # Sensor validation ranges
        self.valid_ranges = {
            'temperature': (15.0, 30.0),  # Celsius
            'humidity': (30.0, 70.0),     # Percentage
            'motion': (False, True)       # Boolean
        }
        
        self.connect()
        logger.info("Pipeline monitor initialized")

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.conn.autocommit = True
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def validate_reading(self, reading: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate a sensor reading."""
        sensor_type = None
        if 'temp_sensor' in reading['sensor_id']:
            sensor_type = 'temperature'
        elif 'humidity_sensor' in reading['sensor_id']:
            sensor_type = 'humidity'
        elif 'motion_sensor' in reading['sensor_id']:
            sensor_type = 'motion'
        
        if not sensor_type:
            return False, "Invalid sensor type"
        
        # Check required fields
        required_fields = ['sensor_id', 'timestamp', 'value']
        if not all(field in reading for field in required_fields):
            self.quality_metrics[sensor_type].missing_values += 1
            return False, "Missing required fields"
        
        # Validate value range
        value_range = self.valid_ranges[sensor_type]
        if not isinstance(reading['value'], (int, float, bool)):
            self.quality_metrics[sensor_type].invalid_readings += 1
            return False, "Invalid value type"
        
        if sensor_type != 'motion':
            if not value_range[0] <= float(reading['value']) <= value_range[1]:
                self.quality_metrics[sensor_type].out_of_range_values += 1
                return False, f"Value out of range [{value_range[0]}, {value_range[1]}]"
        
        self.quality_metrics[sensor_type].total_readings += 1
        return True, None

    def record_batch_metrics(self, batch_size: int, processing_time: float, error_count: int):
        """Record metrics for a batch of readings."""
        self.processing_times.append(processing_time)
        self.error_counts.append(error_count)
        self.batch_sizes.append(batch_size)

    def get_performance_metrics(self) -> PerformanceMetrics:
        """Calculate current performance metrics."""
        if not self.processing_times:
            return PerformanceMetrics()
        
        avg_processing_time = statistics.mean(self.processing_times)
        avg_batch_size = statistics.mean(self.batch_sizes)
        total_readings = sum(self.batch_sizes)
        elapsed_time = time.time() - self.start_time
        readings_per_second = total_readings / elapsed_time if elapsed_time > 0 else 0
        
        return PerformanceMetrics(
            processing_time=avg_processing_time,
            readings_per_second=readings_per_second,
            error_count=sum(self.error_counts),
            batch_size=avg_batch_size
        )

    def get_partition_sizes(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get the size of each partition in the database."""
        try:
            with self.conn.cursor() as cur:
                partition_sizes = {}
                for table in ['temperature_readings', 'humidity_readings', 'motion_events']:
                    cur.execute(f"""
                        SELECT 
                            parent.relname AS table_name,
                            child.relname AS partition_name,
                            pg_size_pretty(pg_relation_size(child.oid)) AS size,
                            pg_stat_get_live_tuples(child.oid) AS row_count
                        FROM pg_inherits
                        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                        WHERE parent.relname = %s
                        ORDER BY child.relname;
                    """, (table,))
                    partition_sizes[table] = [
                        {
                            'partition_name': row[1],
                            'size': row[2],
                            'row_count': row[3]
                        }
                        for row in cur.fetchall()
                    ]
                return partition_sizes
        except Exception as e:
            logger.error(f"Error getting partition sizes: {e}")
            return {}

    def get_data_quality_report(self) -> Dict[str, Dict[str, float]]:
        """Generate a data quality report."""
        report = {}
        for sensor_type, metrics in self.quality_metrics.items():
            if metrics.total_readings == 0:
                continue
            
            report[sensor_type] = {
                'total_readings': metrics.total_readings,
                'error_rate': (metrics.invalid_readings + metrics.missing_values + 
                             metrics.out_of_range_values) / metrics.total_readings * 100,
                'missing_rate': metrics.missing_values / metrics.total_readings * 100,
                'out_of_range_rate': metrics.out_of_range_values / metrics.total_readings * 100
            }
        return report

    def log_metrics(self):
        """Log current pipeline metrics."""
        perf_metrics = self.get_performance_metrics()
        quality_report = self.get_data_quality_report()
        partition_sizes = self.get_partition_sizes()
        
        logger.info("=== Pipeline Metrics ===")
        logger.info(f"Processing Rate: {perf_metrics.readings_per_second:.2f} readings/second")
        logger.info(f"Avg Processing Time: {perf_metrics.processing_time:.3f} seconds")
        logger.info(f"Avg Batch Size: {perf_metrics.batch_size:.1f}")
        logger.info(f"Total Errors: {perf_metrics.error_count}")
        
        logger.info("\n=== Data Quality ===")
        for sensor_type, metrics in quality_report.items():
            logger.info(f"{sensor_type.capitalize()} Sensors:")
            logger.info(f"  Total Readings: {metrics['total_readings']}")
            logger.info(f"  Error Rate: {metrics['error_rate']:.2f}%")
            logger.info(f"  Missing Rate: {metrics['missing_rate']:.2f}%")
            logger.info(f"  Out of Range Rate: {metrics['out_of_range_rate']:.2f}%")
        
        logger.info("\n=== Partition Sizes ===")
        for table, partitions in partition_sizes.items():
            logger.info(f"{table}:")
            for partition in partitions:
                logger.info(f"  {partition['partition_name']}: {partition['size']} "
                          f"({partition['row_count']} rows)")

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Pipeline monitor connection closed") 