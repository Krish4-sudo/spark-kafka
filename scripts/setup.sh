#!/bin/bash

echo "Setting up Weather Streaming Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if .env file exists
if [ ! -f "config/.env" ]; then
    echo "❌ .env file not found. Please create config/.env with your API key."
    exit 1
fi

# Check if API key is set
if ! grep -q "OPENWEATHER_API_KEY=your_api_key_here" config/.env; then
    echo "✅ API key appears to be configured"
else
    echo "❌ Please update your OpenWeather API key in config/.env"
    exit 1
fi

# Create necessary directories
mkdir -p data/output data/checkpoints logs

# Make scripts executable
chmod +x scripts/*.sh

# Build and start services
echo "🏗️  Building Docker images..."
cd docker
docker-compose build

echo "🚀 Starting infrastructure services..."
docker-compose up -d zookeeper kafka kafka-ui

echo "⏳ Waiting for Kafka to be ready..."
sleep 20

# Create Kafka topic
echo "📝 Creating Kafka topic..."
docker-compose exec kafka kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3 \
    --topic weather-data \
    --if-not-exists

echo "✅ Setup complete!"
echo ""
echo "🎯 Next steps:"
echo "1. Start the pipeline: ./scripts/start-pipeline.sh"
echo "2. Monitor Kafka UI: http://localhost:8080"
echo "3. Monitor Spark UI: http://localhost:4040"
echo "4. Stop the pipeline: ./scripts/stop-pipeline.sh"