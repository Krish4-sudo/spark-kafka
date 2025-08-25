#!/bin/bash

echo "🚀 Starting Weather Streaming Pipeline..."

cd docker

# Start all services
docker-compose up -d

echo "⏳ Waiting for services to be ready..."
sleep 20

# Check service health
echo "🔍 Checking service status..."
docker-compose ps

echo ""
echo "✅ Pipeline started successfully!"
echo ""
echo "📊 Monitoring URLs:"
echo "   - Kafka UI: http://localhost:8080"
echo "   - Spark UI: http://localhost:4040"
echo ""
echo "📋 Useful commands:"
echo "   - View logs: docker-compose logs -f [service-name]"
echo "   - Stop pipeline: ./scripts/stop-pipeline.sh"
echo "   - Restart service: docker-compose restart [service-name]"