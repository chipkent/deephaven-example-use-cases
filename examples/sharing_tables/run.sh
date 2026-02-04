#!/bin/bash
set -e

# ------------------------------------------------------------------------------
# Sharing Tables Example - Run Script
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

# Configure Anonymous Authentication (No credentials required)
# Note: Deephaven Core currently requires Anonymous auth for URI resolution.
# See https://github.com/deephaven/deephaven-core/issues/5383

echo "Starting Server 1 (The 'Source' Server) on port 10000..."
# Pipe tail -f /dev/null to stdin to prevent the server from exiting immediately due to EOF
tail -f /dev/null | deephaven server --port 10000 --browser --jvm-args "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" &

echo "Starting Server 2 (The 'Consumer' Server) on port 10001..."
tail -f /dev/null | deephaven server --port 10001 --browser --jvm-args "-DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler" &

# Wait for both background processes
wait
