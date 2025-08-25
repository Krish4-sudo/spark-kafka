#!/bin/bash

echo "🛑 Stopping Weather Streaming Pipeline..."

cd docker

# Stop all services
docker-compose down

echo "✅ Pipeline stopped successfully!"
echo ""
echo "🗑️  To remove all data volumes:"
echo "   docker-compose down -v"