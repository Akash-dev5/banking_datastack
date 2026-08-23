#!/bin/bash

set -e

echo "Pulling Airflow image..."
docker compose --env-file .env -f docker-compose.prod.yml pull

echo "Starting Airflow production stack..."
docker compose --env-file .env -f docker-compose.prod.yml up -d

echo "Checking running containers..."
docker compose --env-file .env -f docker-compose.prod.yml ps

echo "Deployment completed successfully."