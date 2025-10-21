// pybind11 bindings for Black-Scholes implementation
// This file contains only the Python binding code
// The actual implementation is compiled from ../../../../shared/blackscholes/blackscholes.cpp

#include <pybind11/pybind11.h>
#include "../../../../shared/blackscholes/blackscholes.h"

// Define the Python module
PYBIND11_MODULE(blackscholes, m) {
    m.doc() = "Black-Scholes option pricing model";
    
    m.def("norm_cdf", &norm_cdf, "Compute cumulative distribution function for standard normal distribution");
    m.def("norm_pdf", &norm_pdf, "Compute probability density function for standard normal distribution");
    m.def("price", &price, "Compute option price using Black-Scholes formula");
    m.def("delta", &delta, "Compute delta (rate of change of option price with respect to underlying price)");
    m.def("gamma", &gamma, "Compute gamma (rate of change of delta with respect to underlying price)");
    m.def("theta", &theta, "Compute theta (rate of change of option price with respect to time)");
    m.def("vega", &vega, "Compute vega (rate of change of option price with respect to volatility)");
    m.def("rho", &rho, "Compute rho (rate of change of option price with respect to interest rate)");
}
