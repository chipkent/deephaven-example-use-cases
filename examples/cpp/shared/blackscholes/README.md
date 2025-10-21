# Black-Scholes Core Implementation

This directory contains the core C++ implementation of the Black-Scholes option pricing model.

## Files

- **`blackscholes.h`** - Header file with function declarations
- **`blackscholes.cpp`** - Pure C++ implementation (no language bindings)

## Namespace

All functions are contained in the `BlackScholes` namespace to:
- Prevent naming conflicts with standard library functions (e.g., `std::gamma`)
- Provide clean organization and encapsulation
- Enable clean integration with language bindings

## Functions

All functions are in the `BlackScholes` namespace. Access them as `BlackScholes::function_name()`.

### Pricing
- **`price(s, k, r, t, vol, is_call, is_stock)`** - Calculate option price using Black-Scholes formula
  - Returns option price or stock price if `is_stock` is true

### Greeks
- **`delta(s, k, r, t, vol, is_call, is_stock)`** - Sensitivity to underlying price changes (∂V/∂S)
  - Range: 0 to 1 for calls, -1 to 0 for puts
- **`gamma(s, k, r, t, vol, is_stock)`** - Sensitivity of delta to underlying price (∂²V/∂S²)
  - Always positive for long options
- **`theta(s, k, r, t, vol, is_call, is_stock)`** - Sensitivity to time decay (∂V/∂t)
  - Typically negative for long options
- **`vega(s, k, r, t, vol, is_stock)`** - Sensitivity to volatility changes (∂V/∂σ)
  - Always positive for long options
- **`rho(s, k, r, t, vol, is_call, is_stock)`** - Sensitivity to interest rate changes (∂V/∂r)
  - Positive for calls, negative for puts

### Utilities
- **`norm_cdf(x)`** - Cumulative distribution function for standard normal distribution N(0,1)
- **`norm_pdf(x)`** - Probability density function for standard normal distribution N(0,1)

## C++ Usage Example

```cpp
#include "blackscholes.h"

// Calculate call option price
double s = 100.0;    // Underlying price
double k = 95.0;     // Strike price
double r = 0.05;     // Risk-free rate (5%)
double t = 0.6;      // Time to expiry (0.6 years)
double vol = 0.4;    // Volatility (40%)

double call_price = BlackScholes::price(s, k, r, t, vol, true, false);
double call_delta = BlackScholes::delta(s, k, r, t, vol, true, false);
double option_gamma = BlackScholes::gamma(s, k, r, t, vol, false);
```

## Usage

This shared implementation is used directly by all integration examples:

- **`01-blackscholes-javacpp/`** - Compiles this code and wraps it for Java using JavaCPP
  - Build script compiles `blackscholes.cpp` directly
  - JavaCPP parses `blackscholes.h` to generate Java bindings
  
- **`02-blackscholes-pybind11/`** - Compiles this code and wraps it for Python using pybind11
  - `setup.py` compiles `blackscholes.cpp` along with Python bindings
  - Bindings file references `blackscholes.h`
  
- **`03-blackscholes-combined/`** - Uses both wrappers together in Deephaven
  - Builds both JavaCPP and pybind11 integrations from this shared source

**Key Point:** This is the single source of truth for the Black-Scholes implementation. All examples compile this code directly - there are no duplicated copies.
