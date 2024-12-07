import psycopg2
from datetime import datetime, timedelta

def test_query_performance():
    """Test query performance and partition pruning"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            database='iot_db',
            port='5432'
        )
        cursor = conn.cursor()

        # Insert some test data across different months
        print("Inserting test data...")
        base_date = datetime(2024, 12, 1)
        for days in range(90):  # 3 months of data
            test_date = base_date + timedelta(days=days)
            value = 20 + (days % 10)  # Temperature between 20-30
            
            cursor.execute("""
            INSERT INTO temperature_readings (sensor_id, value, timestamp)
            VALUES (%s, %s, %s)
            """, ('temp_sensor_1', value, test_date))

        conn.commit()
        print("Test data inserted")

        # Test queries with different date ranges
        test_queries = [
            ("Single Month Query", """
            EXPLAIN ANALYZE
            SELECT sensor_id, avg(value) as avg_temp
            FROM temperature_readings
            WHERE timestamp >= '2024-12-01' AND timestamp < '2025-01-01'
            GROUP BY sensor_id
            """),
            
            ("Multi-Month Query", """
            EXPLAIN ANALYZE
            SELECT sensor_id, 
                   date_trunc('month', timestamp) as month,
                   avg(value) as avg_temp
            FROM temperature_readings
            WHERE timestamp >= '2024-12-01' AND timestamp < '2025-03-01'
            GROUP BY sensor_id, date_trunc('month', timestamp)
            ORDER BY sensor_id, month
            """),
            
            ("Specific Sensor Query", """
            EXPLAIN ANALYZE
            SELECT date_trunc('day', timestamp) as day,
                   avg(value) as avg_temp
            FROM temperature_readings
            WHERE sensor_id = 'temp_sensor_1'
              AND timestamp >= '2025-01-01'
              AND timestamp < '2025-02-01'
            GROUP BY date_trunc('day', timestamp)
            ORDER BY day
            """)
        ]

        print("\nTesting query performance:")
        for query_name, query in test_queries:
            print(f"\n{query_name}:")
            cursor.execute(query)
            
            # Print execution plan
            for row in cursor.fetchall():
                print(row[0])

        # Test partition pruning
        print("\nTesting partition pruning:")
        cursor.execute("""
        EXPLAIN (ANALYZE, COSTS, TIMING, BUFFERS)
        SELECT count(*) 
        FROM temperature_readings
        WHERE timestamp >= '2025-01-01' AND timestamp < '2025-02-01'
        """)
        
        for row in cursor.fetchall():
            print(row[0])

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
    test_query_performance() 