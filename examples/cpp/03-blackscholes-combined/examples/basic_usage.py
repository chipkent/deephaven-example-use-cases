# Basic usage example - using just the pybind11 integration

from deephaven import empty_table
import blackscholes

# Create a simple table with Black-Scholes calculations using Python/C++
t = empty_table(10).update([
    "UnderlyingPrice = 100 + i",
    "Strike = 95",
    "RiskFree = 0.05",
    "YearsToExpiry = 0.6",
    "Vol = 0.4",
    "IsCall = true",
    "IsStock = false",
    "Price = (double) blackscholes.price(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
])

print("Basic usage example - pybind11 integration")
print(t)
