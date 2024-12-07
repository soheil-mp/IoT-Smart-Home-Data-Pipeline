"""IoT Smart Home Database Package.

This package provides functionality for setting up and managing the IoT Smart Home database,
including table creation, partitioning, and testing.

Modules:
    setup: Database setup and initialization
    tests: Database testing and validation
    utils: Utility functions and configuration
"""

from .setup.setup_db import setup_tables
from .tests.test_db import test_connection, test_tables, test_partitions, test_query_performance
from .utils.db_utils import get_connection, execute_query, create_partition
from .utils.db_config import DB_CONFIG, TABLE_SCHEMAS, INDEX_DEFINITIONS, INITIAL_SENSORS

__all__ = [
    'setup_tables',
    'test_connection',
    'test_tables',
    'test_partitions',
    'test_query_performance',
    'get_connection',
    'execute_query',
    'create_partition',
    'DB_CONFIG',
    'TABLE_SCHEMAS',
    'INDEX_DEFINITIONS',
    'INITIAL_SENSORS'
]

__version__ = '1.0.0'
