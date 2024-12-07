import psycopg2
from datetime import datetime, timedelta

def test_inserts():
    """Test inserting data into different partitions"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            database='iot_db',
            port='5432'
        )
        cursor = conn.cursor()

        # Test dates for different partitions
        test_dates = [
            datetime(2024, 12, 15),  # December 2024
            datetime(2025, 1, 15),   # January 2025
            datetime(2025, 2, 15)    # February 2025
        ]

        # Insert test data for each date
        for test_date in test_dates:
            # Temperature reading
            cursor.execute("""
            INSERT INTO temperature_readings (sensor_id, value, timestamp)
            VALUES (%s, %s, %s)
            RETURNING id
            """, ('temp_sensor_1', 23.5, test_date))
            
            # Humidity reading
            cursor.execute("""
            INSERT INTO humidity_readings (sensor_id, value, timestamp)
            VALUES (%s, %s, %s)
            RETURNING id
            """, ('humidity_sensor_1', 45.0, test_date))
            
            # Motion event
            cursor.execute("""
            INSERT INTO motion_events (sensor_id, detected, timestamp)
            VALUES (%s, %s, %s)
            RETURNING id
            """, ('motion_sensor_1', True, test_date))

        conn.commit()
        print("Test inserts completed successfully")

        # Verify data distribution
        for table in ['temperature_readings', 'humidity_readings', 'motion_events']:
            print(f"\nChecking {table} distribution:")
            cursor.execute(f"""
            SELECT to_char(timestamp, 'YYYY-MM') as month, count(*) 
            FROM {table} 
            GROUP BY to_char(timestamp, 'YYYY-MM')
            ORDER BY month
            """)
            
            for month, count in cursor.fetchall():
                print(f"  {month}: {count} records")

    except Exception as e:
        print(f"Error: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    test_inserts() 