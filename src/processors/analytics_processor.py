import logging
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class SensorStats:
    """Statistical metrics for sensor readings."""
    min_value: float
    max_value: float
    avg_value: float
    std_dev: float
    count: int
    window_start: datetime
    window_end: datetime

class AnalyticsProcessor:
    def __init__(self, db_params: Dict[str, str]):
        """Initialize the analytics processor."""
        self.db_params = db_params
        self.conn = None
        self.connect()
        logger.info("Analytics processor initialized")

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = psycopg2.connect(**self.db_params)
            self.conn.autocommit = False
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def compute_window_stats(self, table: str, window_minutes: int = 5) -> Dict[str, SensorStats]:
        """Compute statistics for each sensor over the specified time window."""
        try:
            window_end = datetime.now()
            window_start = window_end - timedelta(minutes=window_minutes)
            
            with self.conn.cursor() as cur:
                cur.execute(f"""
                    SELECT 
                        sensor_id,
                        MIN(value) as min_value,
                        MAX(value) as max_value,
                        AVG(value) as avg_value,
                        COUNT(*) as count,
                        STDDEV(value) as std_dev
                    FROM {table}
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY sensor_id
                """, (window_start, window_end))
                
                results = {}
                for row in cur.fetchall():
                    sensor_id, min_val, max_val, avg_val, count, std_dev = row
                    results[sensor_id] = SensorStats(
                        min_value=float(min_val) if min_val is not None else 0.0,
                        max_value=float(max_val) if max_val is not None else 0.0,
                        avg_value=float(avg_val) if avg_val is not None else 0.0,
                        std_dev=float(std_dev) if std_dev is not None else 0.0,
                        count=count,
                        window_start=window_start,
                        window_end=window_end
                    )
                return results
        except Exception as e:
            logger.error(f"Error computing window stats for {table}: {e}")
            return {}

    def store_analytics(self, sensor_id: str, metric_name: str, value: float,
                       window_start: datetime, window_end: datetime):
        """Store analytics results in the sensor_analytics table."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO sensor_analytics 
                    (sensor_id, metric_name, value, window_start, window_end)
                    VALUES (%s, %s, %s, %s, %s)
                """, (sensor_id, metric_name, value, window_start, window_end))
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error storing analytics for {sensor_id}: {e}")

    def process_analytics(self, window_minutes: int = 5):
        """Process analytics for all sensor types."""
        # Process temperature readings
        temp_stats = self.compute_window_stats('temperature_readings', window_minutes)
        for sensor_id, stats in temp_stats.items():
            self.store_analytics(sensor_id, 'temp_min', stats.min_value,
                               stats.window_start, stats.window_end)
            self.store_analytics(sensor_id, 'temp_max', stats.max_value,
                               stats.window_start, stats.window_end)
            self.store_analytics(sensor_id, 'temp_avg', stats.avg_value,
                               stats.window_start, stats.window_end)
            self.store_analytics(sensor_id, 'temp_std', stats.std_dev,
                               stats.window_start, stats.window_end)

        # Process humidity readings
        humidity_stats = self.compute_window_stats('humidity_readings', window_minutes)
        for sensor_id, stats in humidity_stats.items():
            self.store_analytics(sensor_id, 'humidity_min', stats.min_value,
                               stats.window_start, stats.window_end)
            self.store_analytics(sensor_id, 'humidity_max', stats.max_value,
                               stats.window_start, stats.window_end)
            self.store_analytics(sensor_id, 'humidity_avg', stats.avg_value,
                               stats.window_start, stats.window_end)
            self.store_analytics(sensor_id, 'humidity_std', stats.std_dev,
                               stats.window_start, stats.window_end)

        # Process motion events (count of detections)
        with self.conn.cursor() as cur:
            window_end = datetime.now()
            window_start = window_end - timedelta(minutes=window_minutes)
            
            cur.execute("""
                SELECT 
                    sensor_id,
                    COUNT(*) as detection_count,
                    SUM(CASE WHEN detected = true THEN 1 ELSE 0 END) as active_count
                FROM motion_events
                WHERE timestamp >= %s AND timestamp < %s
                GROUP BY sensor_id
            """, (window_start, window_end))
            
            for row in cur.fetchall():
                sensor_id, total_count, active_count = row
                if total_count > 0:
                    activity_rate = (active_count / total_count) * 100
                    self.store_analytics(sensor_id, 'motion_rate', activity_rate,
                                       window_start, window_end)

    def get_sensor_trends(self, hours: int = 24) -> Dict[str, List[Dict[str, Any]]]:
        """Get sensor reading trends for the specified time period."""
        trends = {}
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        try:
            # Temperature trends
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        sensor_id,
                        date_trunc('hour', timestamp) as hour,
                        AVG(value) as avg_value,
                        MIN(value) as min_value,
                        MAX(value) as max_value
                    FROM temperature_readings
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY sensor_id, date_trunc('hour', timestamp)
                    ORDER BY hour
                """, (start_time, end_time))
                
                trends['temperature'] = [
                    {
                        'sensor_id': row[0],
                        'hour': row[1],
                        'avg': float(row[2]),
                        'min': float(row[3]),
                        'max': float(row[4])
                    }
                    for row in cur.fetchall()
                ]

            # Humidity trends
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        sensor_id,
                        date_trunc('hour', timestamp) as hour,
                        AVG(value) as avg_value,
                        MIN(value) as min_value,
                        MAX(value) as max_value
                    FROM humidity_readings
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY sensor_id, date_trunc('hour', timestamp)
                    ORDER BY hour
                """, (start_time, end_time))
                
                trends['humidity'] = [
                    {
                        'sensor_id': row[0],
                        'hour': row[1],
                        'avg': float(row[2]),
                        'min': float(row[3]),
                        'max': float(row[4])
                    }
                    for row in cur.fetchall()
                ]

            # Motion trends
            with self.conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        sensor_id,
                        date_trunc('hour', timestamp) as hour,
                        COUNT(*) as total_events,
                        SUM(CASE WHEN detected THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as activity_rate
                    FROM motion_events
                    WHERE timestamp >= %s AND timestamp < %s
                    GROUP BY sensor_id, date_trunc('hour', timestamp)
                    ORDER BY hour
                """, (start_time, end_time))
                
                trends['motion'] = [
                    {
                        'sensor_id': row[0],
                        'hour': row[1],
                        'total_events': row[2],
                        'activity_rate': float(row[3])
                    }
                    for row in cur.fetchall()
                ]
            
            return trends
            
        except Exception as e:
            logger.error(f"Error getting sensor trends: {e}")
            return {}

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Analytics processor connection closed")