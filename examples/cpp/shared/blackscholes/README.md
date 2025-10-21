# Black-Scholes Core Implementation

This directory contains the core C++ implementation of the Black-Scholes option pricing model.

## Files

- **`blackscholes.h`** - Header file with function declarations
- **`blackscholes.cpp`** - Pure C++ implementation (no language bindings)

## Functions

### Pricing
- `price()` - Calculate option price using Black-Scholes formula

### Greeks
- `delta()` - Rate of change of option price with respect to underlying price
- `gamma()` - Rate of change of delta with respect to underlying price
- `theta()` - Rate of change of option price with respect to time
- `vega()` - Rate of change of option price with respect to volatility
- `rho()` - Rate of change of option price with respect to interest rate

### Utilities
- `norm_cdf()` - Cumulative distribution function for standard normal distribution
- `norm_pdf()` - Probability density function for standard normal distribution

## Usage

This code is referenced by the integration examples:
- `01-blackscholes-javacpp/` - Wraps this code for Java using JavaCPP
- `02-blackscholes-pybind11/` - Wraps this code for Python using pybind11
- `03-blackscholes-combined/` - Uses both wrappers in Deephaven

Currently, each example maintains its own copy of this code with integration-specific modifications. In the future, this could be refactored to use this shared implementation directly.
