#!/bin/bash
set -e

echo "Building Docker image for Black-Scholes combined example..."
echo ""

# Build from the cpp examples root directory to have access to all examples
cd ../..

docker build -f 03-blackscholes-combined/docker/Dockerfile \
    -t deephaven-blackscholes:latest \
    .

echo ""
echo "=== Docker Image Built Successfully ==="
echo ""
echo "To run the container:"
echo "  docker run -it --rm -p 10000:10000 deephaven-blackscholes:latest"
echo ""
echo "Or to start a bash shell instead:"
echo "  docker run -it --rm -p 10000:10000 deephaven-blackscholes:latest bash"