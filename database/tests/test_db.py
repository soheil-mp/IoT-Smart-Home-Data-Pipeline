"""Database test module."""
import psycopg2
from datetime import datetime, timedelta

from ..utils.db_config import DB_CONFIG, PARTITIONED_TABLES
from ..utils.db_utils import get_connection, execute_query

def test_connection():
    """Test database connection."""
    try:
        conn = get_connection(DB_CONFIG)
        print("✓ Connection successful")
        return conn
    except Exception as e:
        print(f"✗ Connection failed: {str(e)}")
        raise

def test_tables(conn):
    """Test if all required tables exist."""
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
            execute_query(cursor, f"SELECT 1 FROM {table} LIMIT 1")
            print(f"✓ Table {table} exists and is accessible")
        except Exception as e:
            print(f"✗ Error with table {table}: {str(e)}")
            raise

def test_partitions(conn):
    """Test if partitions are properly set up."""
    cursor = conn.cursor()
    
    for table in PARTITIONED_TABLES:
        try:
            execute_query(
                cursor,
                f"""
                SELECT c.relname as partition_name,
                       pg_get_expr(c.relpartbound, c.oid) as partition_bounds
                FROM pg_class p
                JOIN pg_inherits i ON p.oid = i.inhparent
                JOIN pg_class c ON c.oid = i.inhrelid
                WHERE p.relname = %s
                ORDER BY c.relname;
                """,
                (table,)
            )
            
            partitions = cursor.fetchall()
            if partitions:
                print(f"\n✓ Found {len(partitions)} partitions for {table}")
                for partition_name, partition_bounds in partitions:
                    print(f"  - {partition_name}")
                    print(f"    {partition_bounds}")
                    
                    # Check row count
                    execute_query(cursor, f"SELECT COUNT(*) FROM {partition_name}")
                    count = cursor.fetchone()[0]
                    print(f"    Rows: {count}")
                    
                    # Check indexes
                    execute_query(
                        cursor,
                        """
                        SELECT indexname, indexdef
                        FROM pg_indexes
                        WHERE tablename = %s
                        """,
                        (partition_name,)
                    )
                    
                    indexes = cursor.fetchall()
                    if indexes:
                        print("    Indexes:")
                        for idx_name, idx_def in indexes:
                            print(f"      - {idx_name}")
                            print(f"        {idx_def}")
            else:
                print(f"✗ No partitions found for {table}")
                raise Exception(f"No partitions for {table}")
                
        except Exception as e:
            print(f"✗ Error checking partitions for {table}: {str(e)}")
            raise

def test_query_performance(conn):
    """Test query performance and partition pruning."""
    cursor = conn.cursor()
    try:
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
            execute_query(cursor, query)
            for row in cursor.fetchall():
                print(row[0])

    except Exception as e:
        print(f"✗ Error in query performance test: {str(e)}")
        raise

def main():
    """Run all database tests."""
    conn = None
    try:
        print("\n=== Testing Database Setup ===")
        conn = test_connection()
        
        print("\n=== Testing Table Structure ===")
        test_tables(conn)
        
        print("\n=== Testing Partitions ===")
        test_partitions(conn)
        
        print("\n=== Testing Query Performance ===")
        test_query_performance(conn)
        
        print("\n✓ All database tests completed successfully!")
        
    except Exception as e:
        print(f"\n✗ Tests failed: {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 