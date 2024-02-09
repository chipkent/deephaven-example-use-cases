from deephaven.numpy import to_numpy

date_min = "2024-02-01"
date_max = "2024-02-08"
n_components = 4
symbols = [
    'IBM',
    'MSFT',
    # 'META',
    'INTC',
    'NEM',
    # 'AU',
    # 'AEM',
    # 'GFI'
    'BA',
    'GOOG',
    'CAT',
    'SPY',
    'AAPL',
    'AMZN',
    'NVDA',
    # 'TSLA',
    'LLY',
    'V'
]

import deephaven.dtypes as dht

symbols_array = dht.array(dht.string, symbols)

trades = db.historical_table("FeedOS", "EquityTradeL1") \
    .where(["inRange(Date, date_min, date_max)", "LocalCodeStr in symbols_array"]) \
    .view(["Date", "Timestamp", "Sym=LocalCodeStr", "Price"])

times = trades.view("Timestamp=lowerBin(Timestamp,'PT00:10:00'.toNanos())").select_distinct().sort("Timestamp")

prices_wide = times

for sym in symbols:
    prices_wide = prices_wide.aj(trades.where("Sym == sym"), "Timestamp", f"{sym}=Price")

deltas = prices_wide \
    .drop_columns("Timestamp") \
    .select([f"{sym} = log({sym}) - log({sym}_[ii-1])" for sym in symbols]) \
    .where([f"isFinite({sym})" for sym in symbols])

returns = to_numpy(deltas)

from sklearn.decomposition import PCA
import pandas as pd
from deephaven.pandas import to_table

# Fit PCA model
pca = PCA(n_components=n_components)
pca.fit(returns)
pca_components = pca.components_

from deephaven.table_factory import new_table
from deephaven.column import string_col, double_col

betas = new_table(
    [string_col("Sym", symbols)] + [double_col(f"Factor{i}", pca_components[i]) for i in range(n_components)]
)

