#!/bin/bash
set -e

rm -rf build
mkdir build
cp build.sh build/
cp -r ../javacpp ../pybind11 build/
rm -rf build/{javacpp,pybind11}/{build,venv,dist,blackscholes.egg-info}
docker build -t deephaven-example:latest .

#docker run -it --rm -p 10000:10000 deephaven-example:latest