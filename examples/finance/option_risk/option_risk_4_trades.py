
""" Simulate trades for a given set of underlyings """

import numpy as np
import numpy.typing as npt

from deephaven import time_table
from deephaven.table import Table

def simulate_trades(underlyings: dict[str, float], price_history: Table, update_interval: str) -> Table:
    """ Simulate trades for a given set of underlyings """

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