#!/bin/bash

# Loop until the host is available or timeout is reached
HOST="postgres"
PORT="5432"
TIMEOUT=45

echo "Waiting for $HOST:$PORT to be available for redpanda..."

start_ts=$(date +%s)
while ! nc -z $HOST $PORT; do
  sleep 5
  end_ts=$(date +%s)
  elapsed=$((end_ts - start_ts))
  
  if [[ $elapsed -ge $TIMEOUT ]]; then
    echo "Timeout reached, $HOST:$PORT is still not available!"
    exit 1
  fi
done

echo "$HOST:$PORT is available!"
exec "$@"