#!/bin/bash
set -e

rm -rf build
mkdir build
cp build.sh build/
cp -r ../javacpp ../pybind11 build/
rm -rf build/{javacpp,pybind11}/{build,venv,dist,blackscholes.egg-info}
docker build -t deephaven-example:latest .

#TODO: remove the following line
docker run -it --rm deephaven-example:latest