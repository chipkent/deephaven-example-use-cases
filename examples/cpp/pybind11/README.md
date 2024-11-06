# pybind11 Example 

This is a simple example using [pybind11](https://github.com/pybind/pybind11) to call a C++ function from Python.
[pybind11](https://github.com/pybind/pybind11) is a tool that makes it easy to wrap C++ code in Python.

For more details on [pybind11](https://github.com/pybind/pybind11) see [https://github.com/pybind/pybind11](https://github.com/pybind/pybind11).

## Building

To build this example, you will need to have the following installed:
* Python
* A C++ compiler

To build a wheel file, run the following command:
```bash
./build.sh
```

The build also confirms that the wheel works.

## Running

The build will create a wheel file in the `dist` directory.  This file can be installed with pip.

