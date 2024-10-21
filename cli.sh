#!/bin/bash

# cli.sh

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "Usage: ./cli.sh <command> [args...]"
    echo "Available commands: --help, create-key, delete-key, list-keys, logs-for-key, logs-summary, update-key"
    exit 1
fi

# Run the Docker command with all provided arguments
docker compose --env-file .vali.env -f docker-compose.yml run \
    -e LOCALHOST=false \
    --entrypoint "python src/cli/cli.py $*" \
    control_node