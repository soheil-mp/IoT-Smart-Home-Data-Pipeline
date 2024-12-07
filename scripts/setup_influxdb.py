import os
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

def setup_influxdb():
    """Initialize InfluxDB with required buckets and retention policies."""
    print("Setting up InfluxDB...")
    
    # InfluxDB connection parameters
    url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    token = os.getenv('INFLUXDB_TOKEN', 'iot-pipeline-token-2024')
    org = os.getenv('INFLUXDB_ORG', 'iot_org')
    
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        
        # Get the buckets API
        buckets_api = client.buckets_api()
        
        # Create buckets with retention policies
        buckets = {
            'temperature_metrics': '30d',  # 30 days retention
            'humidity_metrics': '30d',
            'motion_events': '7d',        # 7 days retention
            'system_metrics': '7d'
        }
        
        for bucket_name, retention in buckets.items():
            # Check if bucket exists
            existing_bucket = next(
                (bucket for bucket in buckets_api.find_buckets().buckets
                 if bucket.name == bucket_name),
                None
            )
            
            if not existing_bucket:
                print(f"Creating bucket: {bucket_name}")
                # Convert retention period to seconds
                retention_seconds = int(retention[:-1]) * 24 * 60 * 60
                buckets_api.create_bucket(
                    bucket_name=bucket_name,
                    retention_rules=[{"everySeconds": retention_seconds, "type": "expire"}],
                    org=org
                )
            else:
                print(f"Bucket already exists: {bucket_name}")
        
        print("InfluxDB setup completed successfully!")
        
    except Exception as e:
        print(f"Error setting up InfluxDB: {str(e)}")
        raise
    finally:
        client.close()

if __name__ == "__main__":
    setup_influxdb() 