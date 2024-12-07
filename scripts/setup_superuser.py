"""Script for initial database setup requiring superuser privileges.
This script should be run once by a database administrator."""
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from the root directory
root_dir = Path(__file__).resolve().parent.parent
env_path = root_dir / '.env'
print(f"\nLoading environment from: {env_path}")
print(f"File exists: {env_path.exists()}")

# Force reload environment variables
if env_path.exists():
    with open(env_path) as f:
        env_content = f.read()
        print("\nEnvironment file content:")
        print(env_content)

# Load environment variables with override
load_dotenv(env_path, override=True)

# Print loaded environment variables
print("\nLoaded environment variables:")
print(f"POSTGRES_HOST: {os.getenv('POSTGRES_HOST')}")
print(f"POSTGRES_PORT: {os.getenv('POSTGRES_PORT')}")
print(f"POSTGRES_DB: {os.getenv('POSTGRES_DB')}")
print(f"POSTGRES_USER: {os.getenv('POSTGRES_USER')}")
print(f"POSTGRES_SUPERUSER: {os.getenv('POSTGRES_SUPERUSER')}")

# Database configuration
DB_CONFIG = {
    'host': 'localhost',  # Force localhost for local development
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'user': os.getenv('POSTGRES_USER', 'iot_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'iot_password'),
    'database': os.getenv('POSTGRES_DB', 'iot_db')
}

# Superuser configuration
SUPERUSER_CONFIG = {
    'user': os.getenv('POSTGRES_SUPERUSER', 'postgres'),
    'password': os.getenv('POSTGRES_SUPERUSER_PASSWORD', 'postgres')
}

def setup_database_user():
    """Create database user and grant necessary privileges."""
    conn = None
    cursor = None
    try:
        # Connect to default postgres database as superuser
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database='postgres',
            user=SUPERUSER_CONFIG['user'],
            password=SUPERUSER_CONFIG['password']
        )
        conn.set_isolation_level(0)  # AUTOCOMMIT
        cursor = conn.cursor()
        
        # Create user if not exists
        cursor.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT FROM pg_user WHERE usename = '{DB_CONFIG['user']}') THEN
                    CREATE USER {DB_CONFIG['user']} WITH PASSWORD '{DB_CONFIG['password']}';
                END IF;
            END
            $$;
        """)
        print("Created database user")
        
        # Create database if not exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_CONFIG['database']}'")
        if not cursor.fetchone():
            cursor.execute(f"CREATE DATABASE {DB_CONFIG['database']} OWNER {DB_CONFIG['user']}")
            print("Created database")
        
        # Grant privileges
        cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE {DB_CONFIG['database']} TO {DB_CONFIG['user']}")
        
        # Connect to iot_db to grant schema privileges
        cursor.close()
        conn.close()
        
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            database=DB_CONFIG['database'],
            user=SUPERUSER_CONFIG['user'],
            password=SUPERUSER_CONFIG['password']
        )
        conn.set_isolation_level(0)
        cursor = conn.cursor()
        
        # Grant schema privileges
        cursor.execute(f"GRANT ALL ON SCHEMA public TO {DB_CONFIG['user']}")
        cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO {DB_CONFIG['user']}")
        cursor.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO {DB_CONFIG['user']}")
        print("Granted necessary privileges")
        
    except Exception as e:
        print(f"Error in setup_database_user: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def main():
    """Main function to run superuser setup."""
    print("\n=== Running superuser setup ===")
    print("Using configuration:")
    print(f"Host: {DB_CONFIG['host']}")
    print(f"Port: {DB_CONFIG['port']}")
    print(f"Database: {DB_CONFIG['database']}")
    print(f"User to create: {DB_CONFIG['user']}")
    print(f"Superuser: {SUPERUSER_CONFIG['user']}")
    
    try:
        setup_database_user()
        print("\nSuperuser setup completed successfully!")
        print("\nYou can now run:")
        print("python setup.py")
    except Exception as e:
        print(f"\nSuperuser setup failed: {str(e)}")
        print("\nPlease check:")
        print("1. PostgreSQL is running on the specified host and port")
        print("2. You have provided correct superuser credentials")
        print("3. If using custom credentials, set them in .env:")
        print("   POSTGRES_SUPERUSER=your_superuser")
        print("   POSTGRES_SUPERUSER_PASSWORD=your_password")
        exit(1)

if __name__ == "__main__":
    main() 