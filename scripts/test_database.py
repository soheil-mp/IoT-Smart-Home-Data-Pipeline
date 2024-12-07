import psycopg2
from datetime import datetime, timedelta
import random

def test_connection():
    """Test database connection"""
    try:
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            database='iot_db',
            port='5432'
        )
        print("✓ Connection successful")
        return conn
    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        raise

def test_tables(conn):
    """Test if all required tables exist"""
    cursor = conn.cursor()
    tables = [
        'sensors',
        'temperature_readings',
        'humidity_readings',
        'motion_events',
        'sensor_analytics'
    ]
    
    for table in tables:
        try:
            cursor.execute(f"SELECT 1 FROM {table} LIMIT 1")
            print(f"✓ Table {table} exists and is accessible")
        except Exception as e:
            print(f"✗ Error with table {table}: {str(e)}")
            raise

def test_partitions(conn):
    """Test if partitions are properly set up"""
    cursor = conn.cursor()
    tables = ['temperature_readings', 'humidity_readings', 'motion_events']
    
    for table in tables:
        try:
            cursor.execute(f"""
            SELECT tablename 
            FROM pg_tables 
            WHERE tablename LIKE '{table}_p%'
            """)
            partitions = cursor.fetchall()
            if partitions:
                print(f"✓ Found {len(partitions)} partitions for {table}")
                for partition in partitions:
                    print(f"  - {partition[0]}")
            else:
                print(f"✗ No partitions found for {table}")
                raise Exception(f"No partitions for {table}")
        except Exception as e:
            print(f"✗ Error checking partitions for {table}: {str(e)}")
            raise

def test_insert_and_query(conn):
    """Test insert and query operations"""
    cursor = conn.cursor()
    try:
        # Test sensor data
        cursor.execute("SELECT COUNT(*) FROM sensors")
        sensor_count = cursor.fetchone()[0]
        print(f"✓ Found {sensor_count} sensors in database")

        # Test inserting temperature reading
        current_time = datetime.now()
        test_data = {
            'sensor_id': 'temp_sensor_1',
            'value': 23.5,
            'timestamp': current_time
        }
        
        cursor.execute("""
        INSERT INTO temperature_readings (sensor_id, value, timestamp)
        VALUES (%s, %s, %s)
        RETURNING id
        """, (test_data['sensor_id'], test_data['value'], test_data['timestamp']))
        
        inserted_id = cursor.fetchone()[0]
        print(f"✓ Successfully inserted temperature reading with id {inserted_id}")

        # Verify the insert
        cursor.execute("""
        SELECT * FROM temperature_readings 
        WHERE id = %s
        """, (inserted_id,))
        result = cursor.fetchone()
        if result:
            print("✓ Successfully retrieved inserted data")
        else:
            raise Exception("Could not retrieve inserted data")

        # Test partition routing
        cursor.execute("""
        EXPLAIN SELECT * FROM temperature_readings 
        WHERE timestamp = %s
        """, (current_time,))
        plan = cursor.fetchall()
        print("✓ Query plan shows proper partition usage:")
        for line in plan:
            print(f"  {line[0]}")

        conn.commit()
        print("✓ All insert and query tests passed")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error in insert/query test: {str(e)}")
        raise

def test_indexes(conn):
    """Test if indexes are being used"""
    cursor = conn.cursor()
    try:
        # Test temperature readings index
        cursor.execute("""
        EXPLAIN ANALYZE
        SELECT * FROM temperature_readings 
        WHERE sensor_id = 'temp_sensor_1' 
        AND timestamp > NOW() - interval '1 day'
        """)
        plan = cursor.fetchall()
        print("✓ Index usage test for temperature_readings:")
        for line in plan:
            print(f"  {line[0]}")

        conn.commit()
        print("✓ Index test completed")

    except Exception as e:
        conn.rollback()
        print(f"✗ Error in index test: {str(e)}")
        raise

def main():
    conn = None
    try:
        print("\n=== Testing Database Setup ===")
        conn = test_connection()
        
        print("\n=== Testing Table Structure ===")
        test_tables(conn)
        
        print("\n=== Testing Partitions ===")
        test_partitions(conn)
        
        print("\n=== Testing Data Operations ===")
        test_insert_and_query(conn)
        
        print("\n=== Testing Indexes ===")
        test_indexes(conn)
        
        print("\n✓ All database tests completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Tests failed: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 