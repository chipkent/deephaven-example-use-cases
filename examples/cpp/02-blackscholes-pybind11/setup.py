"""
setup.py - Build configuration for Black-Scholes pybind11 extension

This setup script builds a Python extension module that wraps C++ Black-Scholes
functions using pybind11. It compiles two C++ files together:

1. ../shared/blackscholes/blackscholes.cpp - Core Black-Scholes implementation
2. src/main/cpp/blackscholes_bindings.cpp - pybind11 bindings that expose functions to Python

The resulting module can be imported in Python as:
    import blackscholes
    price = blackscholes.price(100, 95, 0.05, 0.6, 0.4, True, False)

Build process:
    python setup.py bdist_wheel    # Creates a wheel in dist/
    pip install dist/*.whl         # Installs the module

Configuration:
    - C++ standard: Defaults to C++14, override with CMAKE_CXX_STANDARD env var
    - Module name: 'blackscholes'
    - Include paths: pybind11 headers + shared blackscholes directory
"""

from setuptools import setup
import pybind11
import os

# Determine C++ standard to use (default: C++14)
# Can be overridden with environment variable: CMAKE_CXX_STANDARD=17
cxx_std = int(os.environ.get("CMAKE_CXX_STANDARD", "14"))
print(f"Using C++{cxx_std}")

from pybind11.setup_helpers import Pybind11Extension

# Define the extension module
ext_modules = [
    Pybind11Extension(
        'blackscholes',  # Python module name (import blackscholes)
        [
            # Source files to compile
            '../shared/blackscholes/blackscholes.cpp',      # Shared C++ implementation
            'src/main/cpp/blackscholes_bindings.cpp'        # pybind11 Python bindings
        ],
        include_dirs=[
            pybind11.get_include(),      # pybind11 headers
            '../shared/blackscholes'     # For blackscholes.h header file
        ],
        language='c++',
        cxx_std=cxx_std,  # C++ standard version
    ),
]

# Setup configuration
setup(
    name='blackscholes',           # Package name
    ext_modules=ext_modules,       # C++ extension modules to build
)