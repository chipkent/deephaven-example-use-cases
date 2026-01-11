""" Simulate beta values for a set of underlyings.

Beta measures how correlated a security's price movements are with the broader market:
- Beta = 1.0: Moves in line with the market
- Beta > 1.0: More volatile than the market
- Beta < 1.0: Less volatile than the market
- Beta < 0.0: Moves opposite to the market

In risk management, beta is used to calculate market-correlated risk exposure.
This allows portfolio managers to understand how much risk is tied to overall
market movements versus security-specific factors.

This simulation generates random beta values between -0.5 and 1.5 for demonstration.
"""

from deephaven import new_table
from deephaven.table import Table
from deephaven.column import string_col

def simulate_betas(underlyings: dict[str, float]) -> Table:
    """ Simulate beta values for a set of underlyings. """

    return new_table([string_col("USym", list(underlyings.keys()))]) \
        .update(["Beta = random() * 2 - 0.5"])
