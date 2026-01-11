""" Compute option Greeks and jump risk scenarios.

This module calculates:

1. **Standard Greeks** - Sensitivity measures for options:
   - Theo: Theoretical fair value from Black-Scholes
   - Delta: Price sensitivity to underlying movement
   - Gamma: Rate of change of delta
   - Theta: Time decay (daily P&L from time passage)
   - Vega: Sensitivity to volatility changes
   - Rho: Sensitivity to interest rate changes

2. **Jump Risk** - Stress test scenarios:
   - Up10: Option value if underlying jumps up 10%
   - Down10: Option value if underlying drops 10%
   - JumpUp10/JumpDown10: Profit/loss from these sudden moves

Jump risk helps identify vulnerabilities to sudden market shocks.

Greeks are updated periodically (default: every 5 seconds) using the latest market data.
"""

from deephaven import time_table
from deephaven.table import Table

def compute_greeks(current_prices: Table, update_interval: str, rate_risk_free: float) -> Table:
    """ Compute the greeks for the securities """

    return current_prices \
        .snapshot_when(time_table(update_interval).drop_columns("Timestamp")) \
        .update([
            "UMid = (UBid + UAsk) / 2",
            "VolMid = (VolBid + VolAsk) / 2",
            "DT = diffYearsAvg(Timestamp, Expiry)",
            "IsStock = Type == `STOCK`",
            "IsCall = Parity == `CALL`",
            "Theo = black_scholes_price(UMid, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
            "Delta = black_scholes_delta(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
            "Gamma = black_scholes_gamma(UBid, Strike, rate_risk_free, DT, VolBid, IsStock)",
            "Theta = black_scholes_theta(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
            "Vega = black_scholes_vega(UBid, Strike, rate_risk_free, DT, VolBid, IsStock)",
            "Rho = black_scholes_rho(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
            "UMidUp10 = UMid * 1.1",
            "UMidDown10 = UMid * 0.9",
            "Up10 = black_scholes_price(UMidUp10, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
            "Down10 = black_scholes_price(UMidDown10, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
            "JumpUp10 = Up10 - Theo",
            "JumpDown10 = Down10 - Theo",
        ])

