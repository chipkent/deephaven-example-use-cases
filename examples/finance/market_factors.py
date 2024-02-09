from typing import Sequence, Tuple
import numpy as np
import numpy.typing as npt
from deephaven.numpy import to_numpy
from sklearn.decomposition import PCA
from deephaven.table import Table
from deephaven.table_factory import new_table
from deephaven.column import string_col, double_col


def compute_factors(prices: Table, times: Table, symbols: Sequence[str], n_components: int) -> Tuple[Table, npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """ Compute the PCA factors for the given symbols and times.

    Args:
        prices: A table containing the price data for the given symbols.  Must contain columns "Timestamp", and "Sym".
        times: A table containing the times to compute the factors.  Must contain a column "Timestamp".
        symbols: A sequence of symbols to compute the factors for.
        n_components: The number of components to compute.

    Returns:
        A table containing the computed factors, a numpy array of the explained variance ratios, and a numpy array of the cumulative explained variance ratios.
    """
    prices_wide = times.view("Timestamp")

    for sym in symbols:
        prices_wide = prices_wide.aj(prices.where("Sym == sym"), ["Timestamp"], f"{sym}=Price")

    deltas = prices_wide \
        .drop_columns("Timestamp") \
        .select([f"{sym} = log({sym}) - log({sym}_[ii-1])" for sym in symbols]) \
        .where([f"isFinite({sym})" for sym in symbols])

    returns = to_numpy(deltas)

    pca = PCA(n_components=n_components)
    pca.fit(returns)
    pca_components = pca.components_

    betas = new_table(
        [string_col("Sym", symbols)] +
        [double_col(f"Factor{i}", pca_components[i]) for i in range(n_components)]
    )

    pct = pca.explained_variance_ratio_
    cum_pct = np.cumsum(pct)

    return betas, pct, cum_pct



## Example usage on Deephaven Enterprise

date_min = "2024-02-01"
date_max = "2024-02-08"
n_components = 4
symbols = [
    'IBM',
    'MSFT',
    'INTC',
    'NEM',
    'BA',
    'GOOG',
    'CAT',
    'SPY',
    'AAPL',
    'AMZN',
    'NVDA',
    'LLY',
    'V',
    'XOM',
]

import deephaven.dtypes as dht

symbols_array = dht.array(dht.string, symbols)

trades = db.historical_table("FeedOS", "EquityTradeL1") \
    .where(["inRange(Date, date_min, date_max)", "LocalCodeStr in symbols_array"]) \
    .view(["Date", "Timestamp", "Sym=LocalCodeStr", "Price"])

times = trades.view("Timestamp=lowerBin(Timestamp,'PT00:10:00'.toNanos())").select_distinct().sort("Timestamp")

betas, pct, cum_pct = compute_factors(trades, times, symbols, n_components)

