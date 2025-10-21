#!/bin/bash
set -e

# Navigate to the script's directory
cd "$(dirname "$0")/.."

echo "=== Building Combined Black-Scholes Example ==="
echo ""

# remove any existing virtual environment
echo "Removing existing venv/"
rm -rf venv

# create a virtual environment for the demo
echo "Creating venv/"
python3.12 -m venv venv
source venv/bin/activate

# upgrade pip
echo "Upgrading pip..."
python -m pip install --upgrade pip

# install deephaven_server
echo "Installing deephaven_server..."
pip install deephaven_server

echo ""
echo "=== Building pybind11 Integration ==="
# install the pybind11 example integration
pushd ../02-blackscholes-pybind11/
echo "Building pybind11 example integration..."
./build.sh
echo "Installing pybind11 example integration..."
pip install dist/*.whl
popd

echo ""
echo "=== Building JavaCPP Integration ==="
# install the javacpp example integration
pushd ../01-blackscholes-javacpp/
echo "Building javacpp example integration..."
./build.sh
popd

echo "Installing javacpp example integration..."
mkdir -p ./venv/example
cp ../01-blackscholes-javacpp/build/*/{lib*,*.jar} ../01-blackscholes-javacpp/javacpp.jar ./venv/example/

echo ""
echo "=== Build Complete ==="
echo "Virtual environment created at: ./venv"
echo "JavaCPP artifacts copied to: ./venv/example"
echo ""
echo "To start the Deephaven server, run:"
echo "  source venv/bin/activate"
echo '  deephaven server --extra-classpath "./venv/example/blackscholes.jar ./venv/example/javacpp.jar" --jvm-args "-Djava.library.path=./venv/example -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"'
