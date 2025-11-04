
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

# Define the stock symbols and their seed prices
syms = dht.array(dht.string, ["AAPL", "AMZN", "GOOG", "MSFT", "ORCL"])
seed_prices = dht.array(dht.double, [268.0, 255.0, 282.0, 520.0, 258.0])

# Create a real-time ticking table that updates every second
t = time_table("PT1S").update([
    # Cycle through stock symbols
    "SymbolIndex = (int)(ii % syms.length)",
    "Symbol = syms[SymbolIndex]",
    "SeedPrice = seed_prices[SymbolIndex]",
    
    # Add random variation around seed price (gaussian distribution with ~2-3% std deviation)
    "UnderlyingPrice = SeedPrice + randomGaussian(0.0, SeedPrice * 0.025)",
    
    # Create strike prices in $5 intervals around the seed price
    # Randomly select a strike between -25 and +25 from seed (in $5 increments)
    "StrikeOffset = ((int)randomDouble(-5, 6)) * 5",  # -25 to +25 in $5 increments
    "Strike = ((int)(SeedPrice / 5)) * 5 + StrikeOffset",  # Round seed to nearest $5 then add offset
    
    "RiskFree = 0.05",
    "YearsToExpiry = 0.6",
    "Vol = 0.3 + (SymbolIndex % syms.length) * 0.02",
    "IsCall = randomBool()",  # 50% calls, 50% puts
    "IsStock = randomDouble(0.0, 1.0) < 0.2",  # 20% stock positions
    
    # pybind11 integration - all Greeks
    "PricePybind11 = (double) blackscholes.price(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "DeltaPybind11 = (double) blackscholes.delta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "GammaPybind11 = (double) blackscholes.gamma(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "ThetaPybind11 = (double) blackscholes.theta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "VegaPybind11 = (double) blackscholes.vega(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "RhoPybind11 = (double) blackscholes.rho(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    
    # JavaCPP integration - all Greeks  
    "PriceJavaCpp = io.deephaven.BlackScholes.price(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "DeltaJavaCpp = io.deephaven.BlackScholes.delta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "GammaJavaCpp = io.deephaven.BlackScholes.gamma(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "ThetaJavaCpp = io.deephaven.BlackScholes.theta(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "VegaJavaCpp = io.deephaven.BlackScholes.vega(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsStock)",
    "RhoJavaCpp = io.deephaven.BlackScholes.rho(UnderlyingPrice, Strike, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
])\
.reverse()\
.move_columns_up(["Timestamp", "Symbol", "UnderlyingPrice", "Strike", "Vol", "IsCall", "PricePybind11", "PriceJavaCpp", "DeltaPybind11", "DeltaJavaCpp"])
