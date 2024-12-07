"""Database utility functions."""
import psycopg2
from datetime import datetime
from typing import Optional, Tuple
from psycopg2.extensions import connection, cursor

def get_connection(config: dict) -> connection:
    """Create a database connection."""
    return psycopg2.connect(**config)

def get_month_boundaries(date: datetime) -> Tuple[datetime, datetime]:
    """Get the start of current month and start of next month."""
    start_of_month = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    if start_of_month.month == 12:
        start_of_next_month = start_of_month.replace(year=start_of_month.year + 1, month=1)
    else:
        start_of_next_month = start_of_month.replace(month=start_of_month.month + 1)
    
    return start_of_month, start_of_next_month

def execute_query(
    cursor: cursor,
    query: str,
    params: Optional[tuple] = None,
    description: Optional[str] = None
) -> None:
    """Execute a database query with optional parameters and description."""
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if description:
            print(description)
    except Exception as e:
        print(f"Error executing query{f' ({description})' if description else ''}: {str(e)}")
        raise

def create_partition(
    cursor: cursor,
    table_name: str,
    partition_date: datetime,
    description: Optional[str] = None
) -> None:
    """Create a partition for the specified table and date range."""
    try:
        start_date, end_date = get_month_boundaries(partition_date)
        partition_name = f"{table_name}_p{start_date.strftime('%Y_%m')}"
        
        query = f"""
        CREATE TABLE {partition_name}
        PARTITION OF {table_name}
        FOR VALUES FROM ('{start_date.strftime('%Y-%m-%d %H:%M:%S')}') 
        TO ('{end_date.strftime('%Y-%m-%d %H:%M:%S')}')
        """
        execute_query(cursor, query, description=f"Created partition {partition_name}")
        execute_query(
            cursor,
            f"ALTER TABLE {partition_name} OWNER TO iot_user",
            description=f"Set ownership for {partition_name}"
        )
    except Exception as e:
        print(f"Error creating partition: {str(e)}")
        raise 