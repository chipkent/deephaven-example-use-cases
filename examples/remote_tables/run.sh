#!/bin/bash
set -e

# ------------------------------------------------------------------------------
# Remote Tables Example - Run Script
#
# This script launches two independent Deephaven server instances.
# 1. Server 1 (Port 10000): The data source.
# 2. Server 2 (Port 10001): The client/consumer.
#
# It handles cleanup by killing both processes when the script exits.
# ------------------------------------------------------------------------------

# Activate the virtual environment
source venv/bin/activate

# Function to kill child processes on exit
cleanup() {
    echo "Stopping servers..."
    kill 0
}

trap cleanup EXIT

echo "Starting Server 1 (The 'Source' Server) on port 10000..."
deephaven server --port 10000 --browser &

echo "Starting Server 2 (The 'Consumer' Server) on port 10001..."
deephaven server --port 10001 --browser &

# Wait for both background processes
wait
