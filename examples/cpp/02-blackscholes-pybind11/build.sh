#!/bin/bash
set -e

# remove any existing virtual environment
echo "PYBIND11: removing venv"
rm -rf venv

# create a virtual environment for the build
echo "PYBIND11: creating venv"
python3.12 -m venv venv
source venv/bin/activate

# install the required packages
echo "PYBIND11: installing required packages"
python -m pip install --upgrade pip
python -m pip install pybind11 setuptools wheel

# build the wheel
echo "PYBIND11: building the wheel"
python setup.py bdist_wheel

# install and test the wheel
echo "PYBIND11: installing the wheel"
python -m pip install dist/*.whl

echo "PYBIND11: testing the wheel"
python run_it.py
