# Deephaven C++ example

This is a simple example of using C++ in Deephaven.
This example uses:
* [../javacpp](../javacpp) - Integrate C++ code into Java
* [../pybind11](../pybind11) - Integrate C++ code into Python

To build this example, you will need to have the following installed:
* Java
* Python
* A C++ compiler

To build this example, run the following command:
```bash
./build.sh
```

This will create a virtual environment in the `venv` directory with the relevant C++ integrations installed.

To start a Deephaven server with the C++ code, run the following commands:
```bash
source venv/bin/activate
deephaven server --extra-classpath "./venv/example/blackscholes.jar ./venv/example/javacpp.jar" --jvm-args -Djava.library.path=./venv/example 
```

When you are done with the virtual environment, deactivate it:
```bash
deactivate
```

This Python example uses both C++ integrations:
```python

from deephaven import empty_table
import blackscholes

t = empty_table(10).update([
    "UnderlyingPrice = 100+i",
    "Strke = 95",
    "RiskFree = 0.05",
    "YearsToExpiry = 0.6",
    "Vol = 0.4",
    "IsCall = true",
    "IsStock = false",
    "BlackScholesPythonCpp = blackscholes.price(UnderlyingPrice, Strke, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "BlackScholesJavaCpp = io.deephaven.BlackScholes.price(UnderlyingPrice, Strke, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
])
```