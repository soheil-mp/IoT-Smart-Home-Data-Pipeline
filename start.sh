#!/bin/bash

echo "Starting IoT Smart Home Data Pipeline..."

# Function to check if a container is healthy
check_container_health() {
    local container=$1
    local max_attempts=$2
    local attempt=1
    
    echo "Waiting for $container to be healthy..."
    while [ $attempt -le $max_attempts ]; do
        if [ "$(docker inspect --format='{{.State.Health.Status}}' $container)" == "healthy" ]; then
            echo "$container is healthy!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $container not healthy yet, waiting..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $container failed to become healthy within timeout"
    return 1
}

# Set timeout values
CONTAINER_TIMEOUT=60  # seconds to wait for containers
TOTAL_TIMEOUT=300    # total seconds to wait for everything

# Start timer
start_time=$(date +%s)

# Stop any existing containers and remove volumes
echo "Cleaning up existing containers..."
docker-compose down -v

# Start all services
echo "Starting services..."
docker-compose up -d

# Wait for essential services
check_container_health "zookeeper" 12 || exit 1
check_container_health "kafka" 12 || exit 1
check_container_health "postgres" 12 || exit 1
check_container_health "influxdb" 12 || exit 1
check_container_health "grafana" 12 || exit 1

echo "Running setup scripts..."
echo "Setting up IoT database..."

# Run setup script in a Python container within the same network
docker run --rm \
    --network iotsmarthomedatapipeline_default \
    -v "$(pwd):/app" \
    -w /app \
    python:3.10 \
    /bin/bash -c "pip install -r requirements.txt && python scripts/setup_database.py"

if [ $? -eq 0 ]; then
    echo "Database setup completed successfully"
else
    echo "ERROR: Database setup failed"
    exit 1
fi

# Start the applications
echo "Starting applications..."
python src/simulator/sensor_simulator.py &
SIMULATOR_PID=$!
python src/processors/sensor_processor.py &
PROCESSOR_PID=$!

echo "Running for 3 seconds..."
sleep 3

echo "Stopping all processes..."
kill $SIMULATOR_PID $PROCESSOR_PID 2>/dev/null
docker-compose down