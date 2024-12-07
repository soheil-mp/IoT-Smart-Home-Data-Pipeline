-- Create database
CREATE DATABASE iot_db;

\c iot_db;

-- Create extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "btree_gist";

-- Create sensor_data table with partitioning
CREATE TABLE IF NOT EXISTS sensor_data (
    id UUID DEFAULT uuid_generate_v4(),
    sensor_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    value FLOAT NOT NULL,
    unit VARCHAR(20) NOT NULL,
    PRIMARY KEY (id, timestamp)
) PARTITION BY RANGE (timestamp);

-- Create partitions for the next 12 months
DO $$
DECLARE
    start_date TIMESTAMP;
    end_date TIMESTAMP;
    partition_name TEXT;
BEGIN
    start_date := DATE_TRUNC('month', CURRENT_DATE);
    
    FOR i IN 0..11 LOOP
        end_date := start_date + INTERVAL '1 month';
        partition_name := 'sensor_data_' || TO_CHAR(start_date, 'YYYY_MM');
        
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF sensor_data 
            FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );
        
        start_date := end_date;
    END LOOP;
END $$;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_sensor_data_timestamp 
    ON sensor_data USING BRIN (timestamp);
CREATE INDEX IF NOT EXISTS idx_sensor_data_sensor_id 
    ON sensor_data (sensor_id);
CREATE INDEX IF NOT EXISTS idx_sensor_data_location 
    ON sensor_data (location);

-- Create aggregated_metrics table
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(50) NOT NULL,
    location VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    avg_value FLOAT NOT NULL,
    min_value FLOAT NOT NULL,
    max_value FLOAT NOT NULL,
    count INTEGER NOT NULL,
    unit VARCHAR(20) NOT NULL
);

-- Create indexes for aggregated_metrics
CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_timestamp 
    ON aggregated_metrics (timestamp);
CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_sensor_id 
    ON aggregated_metrics (sensor_id);
CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_location 
    ON aggregated_metrics (location);

-- Create pipeline_metrics table
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    component VARCHAR(100) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value FLOAT NOT NULL,
    status VARCHAR(50) NOT NULL
);

-- Create indexes for pipeline_metrics
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_timestamp 
    ON pipeline_metrics (timestamp);
CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_component 
    ON pipeline_metrics (component);

-- Grant privileges on public schema
GRANT ALL PRIVILEGES ON SCHEMA public TO iot_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL PRIVILEGES ON TABLES TO iot_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL PRIVILEGES ON SEQUENCES TO iot_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL PRIVILEGES ON FUNCTIONS TO iot_user;

-- Grant permissions on all objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO iot_user;
 