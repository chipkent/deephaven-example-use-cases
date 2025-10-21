#!/bin/bash
#
# Build script for pybind11 Black-Scholes integration
#
# This script builds a Python extension module that wraps C++ Black-Scholes functions
# using pybind11. The process involves:
# 1. Creating a clean Python virtual environment
# 2. Installing build dependencies (pybind11, setuptools, wheel)
# 3. Building a Python wheel containing the compiled C++ extension
# 4. Installing and testing the wheel
#
# The build compiles two C++ files:
#   - ../shared/blackscholes/blackscholes.cpp (core implementation)
#   - src/main/cpp/blackscholes_bindings.cpp (pybind11 bindings)
#
# Requirements:
#   - Python 3.12 or later
#   - C++ compiler (g++, clang, or MSVC)
#   - C++ standard library
#
# Output:
#   - dist/blackscholes-*.whl - Python wheel package
#   - venv/ - Virtual environment with installed package
#
set -e

# Python interpreter to use (can be overridden: PYTHON=python3.12 ./build.sh)
PYTHON=${PYTHON:-python3}

# ============================================================================
# Check Required Tools
# ============================================================================
command -v ${PYTHON} >/dev/null 2>&1 || { echo "Error: ${PYTHON} not found. Please install Python 3.12+."; exit 1; }
command -v g++ >/dev/null 2>&1 || { echo "Error: g++ not found. Please install a C++ compiler."; exit 1; }

# ============================================================================
# Step 1: Clean Environment
# ============================================================================
# Remove any existing virtual environment and build artifacts to ensure a clean build
echo "PYBIND11: removing venv and dist"
rm -rf venv dist

# ============================================================================
# Step 2: Create Virtual Environment
# ============================================================================
# Create a fresh Python virtual environment for isolated dependency management
echo "PYBIND11: creating venv"
${PYTHON} -m venv venv
source venv/bin/activate

# ============================================================================
# Step 3: Install Build Dependencies
# ============================================================================
# Install the tools needed to build Python C++ extensions
echo "PYBIND11: installing required packages"
python -m pip install --upgrade pip      # Latest pip for best compatibility
python -m pip install build pybind11  # Build tools (using modern 'build' package)

# ============================================================================
# Step 4: Build Python Wheel
# ============================================================================
# Use python -m build (modern standard) to compile C++ and create wheel
# The wheel contains:
#   - Compiled C++ extension module (blackscholes.*.so)
#   - Python metadata
echo "PYBIND11: building the wheel"
python -m build --wheel

# ============================================================================
# Step 5: Install and Test
# ============================================================================
# Install the built wheel into the virtual environment
echo "PYBIND11: installing the wheel"
python -m pip install dist/*.whl

# Run the test script to verify the integration works
echo "PYBIND11: testing the wheel"
python test.py
