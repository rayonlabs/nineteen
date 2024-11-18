#!/bin/bash

if [ -f .vali.env ]; then
  echo "Loading environment variables from .vali.env..."
  set -a
  source .vali.env
  set +a
else
  echo "Error: .vali.env file not found. Exiting."
  exit 1
fi

# Function to check if a string is a valid number
is_valid_number() {
    case $1 in
        ''|*[!0-9]*) return 1 ;;
        *) return 0 ;;
    esac
}

if [ -n "$GRAFANA_USERNAME" ]; then
  GRAFANA_USERNAME=admin
  sed -i '/GRAFANA_USERNAME/d' .vali.env
  echo GRAFANA_USERNAME=$GRAFANA_USERNAME >> .vali.env
fi

if [ -n "$GRAFANA_PASSWORD" ]; then
  GRAFANA_PASSWORD=$(openssl rand -hex 16)
  sed -i '/GRAFANA_PASSWORD/d' .vali.env
  echo GRAFANA_PASSWORD=$GRAFANA_PASSWORD >> .vali.env
fi

if [ -n "$ORGANIC_SERVER_PORT" ] && [ "${ORGANIC_SERVER_PORT,,}" != "none" ]; then
  if is_valid_number "$ORGANIC_SERVER_PORT"; then
    echo "ORGANIC_SERVER_PORT is set to '$ORGANIC_SERVER_PORT'. changing port for nginx."
    ./update-nginx-port.sh -p $ORGANIC_SERVER_PORT
    docker compose --env-file .vali.env -f docker-compose.yml up -d --build --remove-orphans
  else
    echo "ORGANIC_SERVER_PORT is not a valid number. Removing it from .vali.env and starting without entry_node service."
    sed -i '/ORGANIC_SERVER_PORT/d' .vali.env
    docker compose --env-file .vali.env -f docker-compose.yml up -d --build --remove-orphans
  fi
else
  echo "ORGANIC_SERVER_PORT is not set. Starting without entry_node service."
  docker compose --env-file .vali.env -f docker-compose.yml up -d --build --remove-orphans
fi

git stash