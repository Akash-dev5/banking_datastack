#!/bin/bash

set -e

IMAGE_TAG="$1"

if [ -z "$IMAGE_TAG" ]; then
    echo "❌ Error: Airflow image SHA is required."
    echo "Usage: ./ec2-deploy.sh <IMAGE_SHA>"
    exit 1
fi

echo "Deploying Airflow image: $IMAGE_TAG"

echo "Updating AIRFLOW_IMAGE_TAG..."

if grep -q "^AIRFLOW_IMAGE_TAG=" .env; then
    sed -i "s/^AIRFLOW_IMAGE_TAG=.*/AIRFLOW_IMAGE_TAG=$IMAGE_TAG/" .env
else
    echo "AIRFLOW_IMAGE_TAG=$IMAGE_TAG" >> .env
fi

echo "Pulling Airflow image..."

docker compose \
    --env-file .env \
    -f docker-compose.prod.yml \
    pull

echo "Starting Airflow production stack..."

docker compose \
    --env-file .env \
    -f docker-compose.prod.yml \
    up -d

echo "Checking running containers..."

docker compose \
    --env-file .env \
    -f docker-compose.prod.yml \
    ps

echo "✅ Deployment completed successfully."