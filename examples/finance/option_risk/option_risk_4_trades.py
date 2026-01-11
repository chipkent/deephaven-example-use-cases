""" Simulate random trading activity to build portfolio positions.

This module generates a continuous stream of simulated trades that:
- Create realistic portfolio positions across stocks and options
- Mix stock trades (30%) with option trades (70%)
- Use random trade sizes from -1000 to +1000 (positive = buy, negative = sell)
- Execute at realistic bid/ask prices from the market data stream

The simulation:
1. Randomly selects securities (stocks or options)
2. For options, randomly picks strikes and expiries
3. Generates random buy/sell quantities
4. Joins with market data to get execution prices (bid or ask)
5. Filters out trades that couldn't be priced

Output table columns:
- Type: STOCK or OPTION
- USym: Underlying symbol
- Strike/Expiry/Parity: Option details (null for stocks)
- TradeSize: Number of shares/contracts (negative = sell)
- Bid/Ask: Market prices at trade time
- TradePrice: Actual execution price

This builds a realistic trading history for portfolio risk analysis.
"""

import numpy as np
import numpy.typing as npt

from deephaven import time_table
from deephaven.table import Table

def simulate_trades(underlyings: dict[str, float], price_history: Table, update_interval: str) -> Table:
    """ Simulate random trades.
    
    Returns a ticking table with simulated trade executions including price and size.
    """

    usyms_array = dht.array(dht.string, list(underlyings.keys()))
    strikes = {sym: simulate_strikes(underlyings, sym) for sym in underlyings.keys()}

    def get_random_strike(sym: str) -> float:
        """ Get a random strike for a given underlying symbol """
        return np.random.choice(strikes[sym])

    expiry_array = simulate_expiries()

    return time_table(update_interval) \
        .update([
            "Type = random() < 0.3 ? `STOCK` : `OPTION`",
            "USym = usyms_array[randomInt(0, usyms_array.length)]",
            "Strike = Type == `STOCK` ? NULL_DOUBLE : get_random_strike(USym)",
            "Expiry = Type == `STOCK` ? null : expiry_array[randomInt(0, expiry_array.length)]",
            "Parity = Type == `STOCK` ? null : random() < 0.5 ? `CALL` : `PUT`",
            "TradeSize = randomInt(-1000, 1000)",
        ]) \
        .aj(price_history, ["USym", "Strike", "Expiry", "Parity", "Timestamp"], ["Bid", "Ask"]) \
        .update(["TradePrice = random() < 0.5 ? Bid : Ask"])