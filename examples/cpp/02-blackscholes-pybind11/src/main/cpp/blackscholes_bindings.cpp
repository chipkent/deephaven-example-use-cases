/**
 * @file blackscholes_bindings.cpp
 * @brief pybind11 bindings for the Black-Scholes option pricing library
 * 
 * This file contains only the Python binding code that exposes C++ functions to Python.
 * The actual Black-Scholes implementation is in ../../../../shared/blackscholes/blackscholes.cpp
 * and is compiled separately by setup.py.
 * 
 * Architecture:
 * - This file: Defines the Python module interface using pybind11 macros
 * - Shared C++ code: Contains the actual Black-Scholes calculations in BlackScholes namespace
 * - setup.py: Compiles both files together into a Python extension module
 * 
 * The C++ functions are in the BlackScholes namespace and are bound using BlackScholes::
 * prefix. The resulting Python module can be imported as:
 *   import blackscholes
 *   price = blackscholes.price(100, 95, 0.05, 0.6, 0.4, True, False)
 */

#include <pybind11/pybind11.h>
#include "../../../../shared/blackscholes/blackscholes.h"

/**
 * @brief Define the Python module "blackscholes"
 * 
 * This macro creates a Python module that exposes the C++ Black-Scholes functions.
 * Each m.def() call creates a Python function that calls the corresponding C++ function.
 * 
 * @param blackscholes The name of the Python module (import blackscholes)
 * @param m The module object used to define functions
 */
PYBIND11_MODULE(blackscholes, m) {
    m.doc() = "Black-Scholes option pricing model - Python bindings for C++ implementation";
    
    // Statistical functions
    m.def("norm_cdf", &BlackScholes::norm_cdf, 
          "Compute cumulative distribution function for standard normal distribution\n\n"
          "Args:\n"
          "    x (float): Input value\n\n"
          "Returns:\n"
          "    float: CDF value at x");
    
    m.def("norm_pdf", &BlackScholes::norm_pdf, 
          "Compute probability density function for standard normal distribution\n\n"
          "Args:\n"
          "    x (float): Input value\n\n"
          "Returns:\n"
          "    float: PDF value at x");
    
    // Option pricing
    m.def("price", &BlackScholes::price, 
          "Compute option price using Black-Scholes formula\n\n"
          "Args:\n"
          "    s (float): Current underlying price\n"
          "    k (float): Strike price\n"
          "    r (float): Risk-free interest rate (annualized)\n"
          "    t (float): Time to expiration (in years)\n"
          "    vol (float): Volatility (annualized)\n"
          "    is_call (bool): True for call option, False for put option\n"
          "    is_stock (bool): True to return stock price, False for option price\n\n"
          "Returns:\n"
          "    float: Option price");
    
    // Greeks
    m.def("delta", &BlackScholes::delta, 
          "Compute delta (rate of change of option price with respect to underlying price)\n\n"
          "Args:\n"
          "    s (float): Current underlying price\n"
          "    k (float): Strike price\n"
          "    r (float): Risk-free interest rate\n"
          "    t (float): Time to expiration\n"
          "    vol (float): Volatility\n"
          "    is_call (bool): True for call, False for put\n"
          "    is_stock (bool): True to return 1.0, False for option delta\n\n"
          "Returns:\n"
          "    float: Delta value");
    
    m.def("gamma", &BlackScholes::gamma, 
          "Compute gamma (rate of change of delta with respect to underlying price)\n\n"
          "Args:\n"
          "    s (float): Current underlying price\n"
          "    k (float): Strike price\n"
          "    r (float): Risk-free interest rate\n"
          "    t (float): Time to expiration\n"
          "    vol (float): Volatility\n"
          "    is_stock (bool): True to return 0.0, False for option gamma\n\n"
          "Returns:\n"
          "    float: Gamma value");
    
    m.def("theta", &BlackScholes::theta, 
          "Compute theta (rate of change of option price with respect to time)\n\n"
          "Args:\n"
          "    s (float): Current underlying price\n"
          "    k (float): Strike price\n"
          "    r (float): Risk-free interest rate\n"
          "    t (float): Time to expiration\n"
          "    vol (float): Volatility\n"
          "    is_call (bool): True for call, False for put\n"
          "    is_stock (bool): True to return 0.0, False for option theta\n\n"
          "Returns:\n"
          "    float: Theta value (typically negative)");
    
    m.def("vega", &BlackScholes::vega, 
          "Compute vega (rate of change of option price with respect to volatility)\n\n"
          "Args:\n"
          "    s (float): Current underlying price\n"
          "    k (float): Strike price\n"
          "    r (float): Risk-free interest rate\n"
          "    t (float): Time to expiration\n"
          "    vol (float): Volatility\n"
          "    is_stock (bool): True to return 0.0, False for option vega\n\n"
          "Returns:\n"
          "    float: Vega value");
    
    m.def("rho", &BlackScholes::rho, 
          "Compute rho (rate of change of option price with respect to interest rate)\n\n"
          "Args:\n"
          "    s (float): Current underlying price\n"
          "    k (float): Strike price\n"
          "    r (float): Risk-free interest rate\n"
          "    t (float): Time to expiration\n"
          "    vol (float): Volatility\n"
          "    is_call (bool): True for call, False for put\n"
          "    is_stock (bool): True to return 0.0, False for option rho\n\n"
          "Returns:\n"
          "    float: Rho value");
}
