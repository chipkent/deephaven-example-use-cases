from setuptools import setup
import pybind11
import os

cxx_std = int(os.environ.get("CMAKE_CXX_STANDARD", "14"))
print(f"Using C++{cxx_std}")

from pybind11.setup_helpers import Pybind11Extension

ext_modules = [
    Pybind11Extension(
        'blackscholes',
        ['src/main/cpp/blackscholes.cpp'],
        include_dirs=[pybind11.get_include()],
        language='c++',
        cxx_std=cxx_std,
    ),

]

setup(
    name='blackscholes',
    ext_modules=ext_modules,
)