#!/bin/bash
#
# Build script for Combined Black-Scholes Example (Example 3 of 3)
#
# This script builds a complete Deephaven environment with both C++ integration methods:
# 1. pybind11 integration - Allows Python expressions in Deephaven to call C++ functions
# 2. JavaCPP integration - Allows Query Language expressions to call C++ functions
#
# The build process:
# 1. Creates a Python virtual environment
# 2. Installs Deephaven server
# 3. Builds and installs the pybind11 Black-Scholes module
# 4. Builds the JavaCPP Black-Scholes library and copies artifacts to venv
# 5. Provides instructions for starting Deephaven with both integrations
#
# Both integrations compile from the same shared C++ source: ../../shared/blackscholes/
#
# Requirements:
#   - Python 3.12 or later
#   - Java JDK
#   - C++ compiler (g++, clang, or MSVC)
#
# Output:
#   - venv/ - Python virtual environment with Deephaven and pybind11 module
#   - venv/example/ - JavaCPP artifacts (JARs and native libraries)
#
set -e

# Python interpreter to use (can be overridden: PYTHON=python3.12 ./build.sh)
PYTHON=${PYTHON:-python3}

# ============================================================================
# Check Required Tools
# ============================================================================
command -v ${PYTHON} >/dev/null 2>&1 || { echo "Error: ${PYTHON} not found. Please install Python 3.12+."; exit 1; }
command -v java >/dev/null 2>&1 || { echo "Error: java not found. Please install Java JDK."; exit 1; }
command -v g++ >/dev/null 2>&1 || { echo "Error: g++ not found. Please install a C++ compiler."; exit 1; }

# Ensure we're in the correct directory (03-blackscholes-combined/)
cd "$(dirname "$0")"

echo "=== Building Combined Black-Scholes Example ==="
echo ""

# ============================================================================
# Step 1: Create Clean Python Environment
# ============================================================================
# Remove any existing virtual environment to ensure a clean build
echo "Removing existing venv/"
rm -rf venv

# Create a fresh Python virtual environment
echo "Creating venv/"
${PYTHON} -m venv venv
source venv/bin/activate

# Upgrade pip to latest version for best compatibility
echo "Upgrading pip..."
python -m pip install --upgrade pip

# ============================================================================
# Step 2: Install Deephaven Server
# ============================================================================
# Install Deephaven Community Core server into the virtual environment
echo "Installing deephaven_server..."
pip install deephaven_server

# ============================================================================
# Step 3: Build and Install pybind11 Integration
# ============================================================================
# Build the pybind11 Python extension module from shared C++ source
echo ""
echo "=== Building pybind11 Integration ==="
pushd ../02-blackscholes-pybind11/ || { echo "Error: Cannot access pybind11 directory"; exit 1; }
# Clean old wheels to avoid platform mismatch
rm -rf dist/
echo "Building pybind11 example integration..."
./build.sh || { echo "Error: pybind11 build failed"; exit 1; }
echo "Installing pybind11 example integration..."
pip install dist/*.whl || { echo "Error: Failed to install pybind11 wheel"; exit 1; }
popd

# ============================================================================
# Step 4: Build JavaCPP Integration
# ============================================================================
# Build the JavaCPP Java wrapper and native library from shared C++ source
echo ""
echo "=== Building JavaCPP Integration ==="
pushd ../01-blackscholes-javacpp/ || { echo "Error: Cannot access JavaCPP directory"; exit 1; }
echo "Building javacpp example integration..."
./build.sh || { echo "Error: JavaCPP build failed"; exit 1; }
popd

# ============================================================================
# Step 5: Copy JavaCPP Artifacts to Virtual Environment
# ============================================================================
# Copy JavaCPP JARs and native libraries to a location accessible by Deephaven
echo "Installing javacpp example integration..."
mkdir -p ./venv/example
# Copy: native libraries (lib*), blackscholes.jar, and javacpp.jar
cp ../01-blackscholes-javacpp/build/*/{lib*,*.jar} ../01-blackscholes-javacpp/javacpp.jar ./venv/example/

# ============================================================================
# Build Complete - Display Instructions
# ============================================================================
echo ""
echo "=== Build Complete ==="
echo "Virtual environment created at: ./venv"
echo "JavaCPP artifacts copied to: ./venv/example"
echo ""
echo "To start the Deephaven server, run:"
echo "  source venv/bin/activate"
echo '  deephaven server --extra-classpath "./venv/example/blackscholes.jar ./venv/example/javacpp.jar" --jvm-args "-Djava.library.path=./venv/example -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"'
echo ""
echo "Then open http://localhost:10000 in your browser"
