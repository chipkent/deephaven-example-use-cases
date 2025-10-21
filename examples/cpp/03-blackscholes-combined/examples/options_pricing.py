
from deephaven import time_table
import deephaven.dtypes as dht
import blackscholes

"""
Real-time Options Pricing Example

This example demonstrates both pybind11 and JavaCPP C++ integrations working together
in Deephaven. It calculates Black-Scholes option prices and Greeks for multiple stocks
in real-time, showing that both integration methods produce identical results.

Features:
- Real-time ticking data (updates every second)
- Multiple stocks: AAPL, AMZN, GOOG, MSFT, ORCL
- All Greeks: Price, Delta, Gamma, Theta, Vega, Rho
- Side-by-side comparison of pybind11 vs JavaCPP

Both integrations call the same underlying C++ code (BlackScholes namespace functions),
demonstrating that you can use either integration method based on your needs:
- pybind11: For Python expressions and scripts
- JavaCPP: For Query Language expressions
"""

# Define the stock symbols to simulate
syms = dht.array(dht.string, ["AAPL", "AMZN", "GOOG", "MSFT", "ORCL"])

# Create a real-time ticking table that updates every second
t = time_table("PT1S").update([
    # Cycle through stock symbols
    "Symbol = syms[ (int)(ii % syms.length) ]",

    # Simulate realistic market data with price oscillation
    "UnderlyingPrice = 100 + (ii % (10 * syms.length)) + Math.sin(ii * 0.1) * 10",
    "Strike = 95",
    "RiskFree = 0.05",  # 5% risk-free rate
    "YearsToExpiry = 0.6",  # 0.6 years to expiration
    "Vol = 0.3 + (ii % syms.length) * 0.02",  # Volatility varies by stock
    "IsCall = randomBool()",  # 50% calls, 50% puts (random)
    "IsStock = randomDouble(0.0, 1.0) < 0.2",  # 20% stock positions, 80% options (random)

    # Calculate all Greeks using pybind11 (Python C++ integration)
    "PricePybind11 = (double) blackscholes.price(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "DeltaPybind11 = (double) blackscholes.delta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "GammaPybind11 = (double) blackscholes.gamma(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "ThetaPybind11 = (double) blackscholes.theta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "VegaPybind11 = (double) blackscholes.vega(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "RhoPybind11 = (double) blackscholes.rho(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",

    # Calculate all Greeks using JavaCPP (Query Language C++ integration)
    "PriceJavaCpp = io.deephaven.BlackScholes.price(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "DeltaJavaCpp = io.deephaven.BlackScholes.delta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "GammaJavaCpp = io.deephaven.BlackScholes.gamma(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "ThetaJavaCpp = io.deephaven.BlackScholes.theta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "VegaJavaCpp = io.deephaven.BlackScholes.vega(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "RhoJavaCpp = io.deephaven.BlackScholes.rho(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
])
