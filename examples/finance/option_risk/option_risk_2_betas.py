
""" Simulate beta values for a set of underlyings. """

from deephaven import new_table
from deephaven.table import Table
from deephaven.column import string_col

def simulate_betas(underlyings: dict[str, float]) -> Table:
    """ Simulate beta values for a set of underlyings. """

    return new_table([string_col("USym", list(underlyings.keys()))]) \
        .update(["Beta = random() * 2 - 0.5"])
