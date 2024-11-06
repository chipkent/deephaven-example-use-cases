#!/bin/bash
set -e

# create a virtual environment for the build
python3.12 -m venv venv
source venv/bin/activate

# install the required packages
python -m pip install --upgrade pip
python -m pip install pybind11 setuptools wheel

# build the wheel
python setup.py bdist_wheel

# install and test the wheel
python -m pip install dist/*.whl
python run_it.py
