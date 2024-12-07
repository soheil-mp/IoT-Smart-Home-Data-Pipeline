import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_database():
    """Create the database and user if they don't exist."""
    conn = None
    cursor = None
    try:
        # Connect using iot_user credentials
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            port='5432',
            database='postgres'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Create database if not exists
        cursor.execute("""
        SELECT 1 FROM pg_database WHERE datname = 'iot_db'
        """)
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE iot_db")
        
        # Connect to the iot_db
        cursor.close()
        conn.close()
        
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            port='5432',
            database='iot_db'
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        print("Database setup completed")

    except Exception as e:
        print(f"Error in create_database: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def get_month_boundaries(date):
    """Get the start of current month and start of next month"""
    from calendar import monthrange
    
    # Get first day of the month
    start_of_month = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Get first day of next month
    if start_of_month.month == 12:
        start_of_next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
    else:
        start_of_next_month = start_of_month.replace(month=start_of_month.month + 1)
    
    return start_of_month, start_of_next_month

def setup_tables():
    """Create the required tables in the database."""
    conn = None
    cursor = None
    try:
        # Connect to the IoT database using environment variables
        conn = psycopg2.connect(
            host='localhost',
            user='iot_user',
            password='iot_password',
            database='iot_db',
            port='5432'
        )
        conn.autocommit = False  # Enable transaction management
        cursor = conn.cursor()
        print("Successfully connected to the database")

        # Drop all existing tables to start fresh
        cursor.execute("""
        DROP TABLE IF EXISTS 
            temperature_readings,
            humidity_readings,
            motion_events,
            sensor_analytics,
            sensors
        CASCADE
        """)
        print("Cleaned up existing tables")

        # Create sensors table
        cursor.execute("""
        CREATE TABLE sensors (
            sensor_id VARCHAR(50) PRIMARY KEY,
            type VARCHAR(20) NOT NULL,
            location VARCHAR(100),
            status VARCHAR(20) DEFAULT 'active',
            last_reading TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("ALTER TABLE sensors OWNER TO iot_user")
        print("Created sensors table")

        # Create partitioned tables
        tables_schema = {
            'temperature_readings': """
                id SERIAL,
                sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
                value DECIMAL(5,2) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """,
            'humidity_readings': """
                id SERIAL,
                sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
                value DECIMAL(5,2) NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """,
            'motion_events': """
                id SERIAL,
                sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
                detected BOOLEAN NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            """
        }

        for table_name, schema in tables_schema.items():
            cursor.execute(f"""
            CREATE TABLE {table_name} (
                {schema},
                PRIMARY KEY (id, timestamp)
            ) PARTITION BY RANGE (timestamp)
            """)
            cursor.execute(f"ALTER TABLE {table_name} OWNER TO iot_user")
            print(f"Created {table_name} table")

        # Create sensor_analytics table
        cursor.execute("""
        CREATE TABLE sensor_analytics (
            id SERIAL PRIMARY KEY,
            sensor_id VARCHAR(50) REFERENCES sensors(sensor_id),
            metric_name VARCHAR(50) NOT NULL,
            value DECIMAL(10,2) NOT NULL,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        cursor.execute("ALTER TABLE sensor_analytics OWNER TO iot_user")
        print("Created sensor_analytics table")

        # Create dynamic partitions for each table (current month and next 2 months)
        from datetime import datetime, timedelta
        
        # Start from current month
        current_date = datetime.now()
        base_date = current_date.replace(day=1)
        
        for table in ['temperature_readings', 'humidity_readings', 'motion_events']:
            # Create partitions for current month and next two months
            for i in range(3):
                if i == 0:
                    partition_date = base_date
                else:
                    # Calculate next month's date
                    if base_date.month + i <= 12:
                        partition_date = base_date.replace(month=base_date.month + i)
                    else:
                        # Handle year transition
                        new_year = base_date.year + ((base_date.month + i - 1) // 12)
                        new_month = ((base_date.month + i - 1) % 12) + 1
                        partition_date = base_date.replace(year=new_year, month=new_month)
                
                start_date, end_date = get_month_boundaries(partition_date)
                partition_name = f"{table}_p{start_date.strftime('%Y_%m')}"
                
                try:
                    cursor.execute(f"""
                    CREATE TABLE {partition_name}
                    PARTITION OF {table}
                    FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d %H:%M:%S')}') 
                    TO ('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')
                    """)
                    cursor.execute(f"ALTER TABLE {partition_name} OWNER TO iot_user")
                    print(f"Created partition {partition_name} for range {start_date} to {end_date}")
                except Exception as e:
                    print(f"Warning: Could not create partition {partition_name}: {str(e)}")
                    continue

        # Create indexes
        index_definitions = [
            ("idx_temp_sensor_timestamp", "temperature_readings(sensor_id, timestamp)"),
            ("idx_humidity_sensor_timestamp", "humidity_readings(sensor_id, timestamp)"),
            ("idx_motion_sensor_timestamp", "motion_events(sensor_id, timestamp)"),
            ("idx_analytics_sensor_metric", "sensor_analytics(sensor_id, metric_name, window_start)")
        ]

        for index_name, index_def in index_definitions:
            try:
                cursor.execute(f"CREATE INDEX {index_name} ON {index_def}")
                print(f"Created index {index_name}")
            except Exception as e:
                print(f"Warning: Could not create index {index_name}: {str(e)}")
                continue

        # Insert initial sensor metadata
        sensors = [
            ('temp_sensor_1', 'temperature', 'Living Room'),
            ('temp_sensor_2', 'temperature', 'Kitchen'),
            ('temp_sensor_3', 'temperature', 'Bedroom'),
            ('temp_sensor_4', 'temperature', 'Office'),
            ('temp_sensor_5', 'temperature', 'Bathroom'),
            ('humidity_sensor_1', 'humidity', 'Living Room'),
            ('humidity_sensor_2', 'humidity', 'Kitchen'),
            ('humidity_sensor_3', 'humidity', 'Bedroom'),
            ('humidity_sensor_4', 'humidity', 'Office'),
            ('humidity_sensor_5', 'humidity', 'Bathroom'),
            ('motion_sensor_1', 'motion', 'Living Room'),
            ('motion_sensor_2', 'motion', 'Kitchen'),
            ('motion_sensor_3', 'motion', 'Bedroom'),
            ('motion_sensor_4', 'motion', 'Office'),
            ('motion_sensor_5', 'motion', 'Bathroom')
        ]

        for sensor in sensors:
            try:
                cursor.execute("""
                INSERT INTO sensors (sensor_id, type, location)
                VALUES (%s, %s, %s)
                ON CONFLICT (sensor_id) DO UPDATE 
                SET type = EXCLUDED.type,
                    location = EXCLUDED.location,
                    updated_at = CURRENT_TIMESTAMP
                """, sensor)
            except Exception as e:
                print(f"Warning: Could not insert sensor {sensor[0]}: {str(e)}")
                continue

        print("Inserted sensor metadata")
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

if __name__ == "__main__":
    print("Testing database setup...")
    try:
        print("\n1. Creating database...")
        create_database()
        print("Database creation successful!")
        
        print("\n2. Setting up tables...")
        setup_tables()
        print("Table setup successful!")
        
        print("\nComplete database setup was successful!")
    except Exception as e:
        print(f"Setup failed: {str(e)}")
        exit(1) 