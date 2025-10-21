
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
    "BlackScholesPythonCpp = (double) blackscholes.price(UnderlyingPrice, Strke, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
    "BlackScholesJavaCpp = io.deephaven.BlackScholes.price(UnderlyingPrice, Strke, RiskFree, YearsToExpiry, Vol, IsCall, IsStock)",
])
