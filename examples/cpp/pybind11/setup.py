from setuptools import setup, Extension
import pybind11
import os

cxx_std = int(os.environ.get("CMAKE_CXX_STANDARD", "14"))

from pybind11.setup_helpers import ParallelCompile, Pybind11Extension

ext_modules = [
    # Extension(
    #     'example',
    #     ['example.cpp'],
    #     include_dirs=[pybind11.get_include()],
    #     language='c++',
    #     cxx_std=cxx_std,
    # ),

    # Pybind11Extension(
    #     'example',
    #     ['example.cpp'],
    #     include_dirs=[pybind11.get_include()],
    #     language='c++',
    #     cxx_std=cxx_std,
    # ),

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