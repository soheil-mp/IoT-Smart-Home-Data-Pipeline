"""Main database setup module."""
from datetime import datetime
from typing import Optional

from ..utils.db_config import (
    DB_CONFIG,
    TABLE_SCHEMAS,
    INDEX_DEFINITIONS,
    INITIAL_SENSORS,
    PARTITIONED_TABLES
)
from ..utils.db_utils import get_connection, execute_query, create_partition

def setup_tables() -> None:
    """Create the required tables in the database."""
    conn = None
    cursor = None
    try:
        conn = get_connection(DB_CONFIG)
        conn.autocommit = False
        cursor = conn.cursor()
        
        # Drop existing tables
        execute_query(
            cursor,
            f"""
            DROP TABLE IF EXISTS 
                {','.join(TABLE_SCHEMAS.keys())}
            CASCADE
            """,
            description="Cleaned up existing tables"
        )
        
        # Create tables
        for table_name, schema in TABLE_SCHEMAS.items():
            if table_name in PARTITIONED_TABLES:
                query = f"""
                CREATE TABLE {table_name} (
                    {schema},
                    PRIMARY KEY (id, timestamp)
                ) PARTITION BY RANGE (timestamp)
                """
            else:
                query = f"""
                CREATE TABLE {table_name} (
                    {schema}
                )
                """
            execute_query(cursor, query, description=f"Created {table_name} table")
        
        # Create partitions
        current_date = datetime.now()
        base_date = current_date.replace(day=1)
        
        for table in PARTITIONED_TABLES:
            for i in range(3):
                if i == 0:
                    partition_date = base_date
                else:
                    if base_date.month + i <= 12:
                        partition_date = base_date.replace(month=base_date.month + i)
                    else:
                        new_year = base_date.year + ((base_date.month + i - 1) // 12)
                        new_month = ((base_date.month + i - 1) % 12) + 1
                        partition_date = base_date.replace(year=new_year, month=new_month)
                
                create_partition(cursor, table, partition_date)
        
        # Create indexes
        for index_name, index_def in INDEX_DEFINITIONS:
            execute_query(
                cursor,
                f"CREATE INDEX {index_name} ON {index_def}",
                description=f"Created index {index_name}"
            )
        
        # Insert initial sensor data
        for sensor in INITIAL_SENSORS:
            execute_query(
                cursor,
                """
                INSERT INTO sensors (sensor_id, type, location)
                VALUES (%s, %s, %s)
                ON CONFLICT (sensor_id) DO UPDATE 
                SET type = EXCLUDED.type,
                    location = EXCLUDED.location,
                    updated_at = CURRENT_TIMESTAMP
                """,
                sensor
            )
        
        conn.commit()
        print("Tables created and initialized successfully!")
        
    except Exception as e:
        print(f"Error setting up tables: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """Main setup function."""
    print("Setting up IoT database tables...")
    try:
        setup_tables()
        print("Database setup completed successfully!")
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        exit(1)

if __name__ == "__main__":
    main() 