""" Simulate real-time market prices for stocks and options.

This module generates continuous streaming market data including:
- Bid/ask prices for underlying stocks
- Implied volatility (bid/ask spread)
- Option prices calculated from Black-Scholes model

The simulation:
1. Generates random walk price movements for underlying stocks
2. Simulates volatility changes over time
3. Calculates option prices using the Black-Scholes model with simulated inputs
4. Creates bid/ask spreads for both stocks and options

This produces realistic ticking market data for demonstrating real-time risk calculations.
"""

import numpy as np

from deephaven import empty_table, time_table, merge, dtypes as dht
from deephaven.table import Table

def simulate_market_prices(underlyings: dict[str, float], sec_master: Table, update_interval: str, rate_risk_free: float) -> Table:
    """ Simulate streaming market prices.
    
    Returns a ticking table with real-time bid/ask prices and volatility for all securities.
    """

    usyms_array = dht.array(dht.string, list(underlyings.keys()))
    last_price = {usym: round(np.abs(np.random.normal(open, 30.0)), 2) for usym, open in underlyings.items()}
    last_vol = {usym: np.abs(np.random.normal(0.4, 0.2)) + 0.03 for usym in underlyings.keys()}

    def gen_sym() -> str:
        """ Generate a random symbol """
        return np.random.choice(usyms_array)

    def gen_price(sym: str) -> float:
        """ Generate a random price for a given symbol """
        p = last_price[sym]
        p += (np.random.random() - 0.5)
        p = abs(p)
        last_price[sym] = p
        return round(p, 2)

    def gen_vol(sym: str) -> float:
        """ Generate a random volatility for a given symbol """
        v = last_vol[sym]
        v += (np.random.random() - 0.5) * 0.01
        v = abs(v)
        last_vol[sym] = v
        return v

    underlying_prices = time_table(update_interval) \
        .update([
            "Type = `STOCK`",
            "USym = gen_sym()",
            "Strike = NULL_DOUBLE",
            "Expiry = (Instant) null",
            "Parity = (String) null",
            "UBid = gen_price(USym)",
            "UAsk = UBid + randomInt(1, 10)*0.01",
            "VolBid = gen_vol(USym)",
            "VolAsk = VolBid + randomInt(1, 10)*0.01",
            "Bid = UBid",
            "Ask = UAsk",
        ])

    option_securities = sec_master.where("Type = `OPTION`")

    option_prices = underlying_prices \
        .view(["Timestamp", "USym", "UBid", "UAsk", "VolBid", "VolAsk"]) \
        .join(option_securities, "USym") \
        .view(["Timestamp", "Type", "USym", "Strike", "Expiry", "Parity", "UBid", "UAsk", "VolBid", "VolAsk"]) \
        .update([
            "DT = diffYearsAvg(Timestamp, Expiry)",
            "IsStock = Type == `STOCK`",
            "IsCall = Parity == `CALL`",
            "Bid = black_scholes_price(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
            "Ask = black_scholes_price(UAsk, Strike, rate_risk_free, DT, VolAsk, IsCall, IsStock)",
        ]) \
        .drop_columns(["DT", "IsStock", "IsCall"])

    return merge([underlying_prices, option_prices]).sort("Timestamp")
