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

echo "Starting Server 1 (The 'Source' Server) on port 10000..."
# CRITICAL: We pipe 'tail -f /dev/null' into the server to keep the standard input (stdin) open.
# When running in the background ('&'), the server would otherwise immediately encounter
# an "End of File" (EOF) on stdin and shut down to prevent hanging resources.
tail -f /dev/null | deephaven server --port 10000 --browser &

echo "Starting Server 2 (The 'Consumer' Server) on port 10001..."
# CRITICAL: We pipe 'tail -f /dev/null' into the server to keep the standard input (stdin) open.
# When running in the background ('&'), the server would otherwise immediately encounter
# an "End of File" (EOF) on stdin and shut down to prevent hanging resources.
tail -f /dev/null | deephaven server --port 10001 --browser &

# Wait for both background processes
wait
