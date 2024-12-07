import psycopg2
from datetime import datetime

def check_partitions():
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            database='iot_db',
            port='5432'
        )
        cursor = conn.cursor()

        # Get partition information for each table
        tables = ['temperature_readings', 'humidity_readings', 'motion_events']
        
        for table in tables:
            print(f"\nChecking partitions for {table}:")
            
            # Get partition information
            cursor.execute(f"""
            SELECT c.relname as partition_name,
                   pg_get_expr(c.relpartbound, c.oid) as partition_bounds
            FROM pg_class p
            JOIN pg_inherits i ON p.oid = i.inhparent
            JOIN pg_class c ON c.oid = i.inhrelid
            WHERE p.relname = %s
            ORDER BY c.relname;
            """, (table,))
            
            partitions = cursor.fetchall()
            
            for partition_name, partition_bounds in partitions:
                print(f"\nPartition: {partition_name}")
                print(f"Bounds: {partition_bounds}")
                
                # Get row count for this partition
                cursor.execute(f"SELECT COUNT(*) FROM {partition_name}")
                count = cursor.fetchone()[0]
                print(f"Row count: {count}")
                
                # Get index information
                cursor.execute(f"""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = %s
                """, (partition_name,))
                
                indexes = cursor.fetchall()
                if indexes:
                    print("Indexes:")
                    for idx_name, idx_def in indexes:
                        print(f"  - {idx_name}")
                        print(f"    {idx_def}")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    check_partitions() 