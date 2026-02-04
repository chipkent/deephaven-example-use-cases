#!/bin/bash
set -e

# ------------------------------------------------------------------------------
# Remote Tables Example - Setup Script
#
# This script prepares the Python environment for the example.
# 1. Creates a virtual environment ('venv') if not present.
# 2. Installs the required Deephaven packages.
# ------------------------------------------------------------------------------

# Create a virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate the virtual environment
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install deephaven-server

echo "Setup complete."
