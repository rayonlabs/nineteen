#!/bin/bash

set -e

wait_for_postgres() {
  local retries=5
  local wait=5
  local attempt=1

  echo "Waiting for PostgreSQL to be ready..."

  while [ $attempt -le $retries ]; do
    if docker exec 0_test_db pg_isready -U test_user -d test_db; then
      echo "PostgreSQL is ready!"
      return 0
    else
      echo "PostgreSQL is not ready yet. Waiting $wait seconds..."
      sleep $wait
    fi
    attempt=$((attempt + 1))
  done

  echo "PostgreSQL is still not ready after $retries attempts."
  return 1
}

# Function to run dbmate migrations
run_dbmate_migrations() {
  echo "Running dbmate migrations..."
  docker compose -f docker-compose.yml -f docker-compose.testing.yml run --rm dbmate up
  echo "dbmate migrations completed."
}

# Main script execution
main() {
  # Wait for PostgreSQL to be ready
  wait_for_postgres

  # Run dbmate migrations
  run_dbmate_migrations
}

main
