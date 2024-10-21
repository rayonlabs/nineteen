#!/bin/bash

set -a
source .vali.env
set +a

# Function to check if a string is a valid number
is_valid_number() {
    case $1 in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

# Check if ORGANIC_SERVER_PORT is set to 'none' - for legacy config reasons
if [ -n "$ORGANIC_SERVER_PORT" ] && [ "${ORGANIC_SERVER_PORT,,}" != "none" ]; then
  if is_valid_number "$ORGANIC_SERVER_PORT"; then
    echo "ORGANIC_SERVER_PORT is set to '$ORGANIC_SERVER_PORT'. Starting entry_node service."
    docker compose --env-file .vali.env -f docker-compose.yml --profile entry_node_profile up -d --build --remove-orphans
  else
    echo "ORGANIC_SERVER_PORT is not a valid number. Removing it from .vali.env and starting without entry_node service."
    sed -i '/ORGANIC_SERVER_PORT/d' .vali.env
    docker compose --env-file .vali.env -f docker-compose.yml up -d --build --remove-orphans
  fi
else
  echo "ORGANIC_SERVER_PORT is not set. Starting without entry_node service."
  docker compose --env-file .vali.env -f docker-compose.yml up -d --build --remove-orphans
fi