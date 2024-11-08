#!/bin/bash
set -e

# remove any existing virtual environment
echo "removing venv/"
rm -rf venv

# create a virtual environment for the demo
echo "creating venv/"
python3.12 -m venv venv
source venv/bin/activate

# upgrade pip
python -m pip install --upgrade pip

# install deephaven_server
pip install deephaven_server

# install the pybind11 example integration
pushd ../pybind11/
echo "DEEPHAVEN EXAMPLE: building pybind11 example integration"
./build.sh
echo "DEEPHAVEN EXAMPLE: installing pybind11 example integration"
pip install dist/*.whl
popd

# install the javacpp example integration
pushd ../javacpp/
echo "DEEPHAVEN EXAMPLE: building javacpp example integration"
./build.sh
popd
echo "DEEPHAVEN EXAMPLE: installing javacpp example integration"
mkdir ./venv/example
cp ../javacpp/build/*/{lib*,*.jar} ../javacpp/javacpp.jar ./venv/example/
