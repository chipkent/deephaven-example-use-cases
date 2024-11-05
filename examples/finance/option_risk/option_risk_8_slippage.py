
""" Compute the slippage for the given trade. Slippage is defined as price movement in a short period after the trade. """

from deephaven.table import Table, RollupTable
from deephaven import agg

def compute_slippage(trade_history: Table, price_history: Table, holding_period: str) -> RollupTable:
    """ Compute the slippage for the given trade. Slippage is defined as price movement in a short period after the trade. """

    trade_pnl = trade_history \
        .view(["Timestamp", "USym", "Strike", "Expiry", "Parity", "TradeSize", "TradePrice"]) \
        .aj(price_history.update(f"Timestamp=Timestamp-'{holding_period}'"), ["USym", "Strike", "Expiry", "Parity", "Timestamp"],
            ["FutureBid=Bid", "FutureAsk=Ask"]) \
        .update([
            "FutureMid = (FutureBid + FutureAsk) / 2",
            "PriceChange = FutureMid - TradePrice",
            "PnL = TradeSize * PriceChange",
        ])

    by = ["USym", "Expiry", "Strike", "Parity"]

    return trade_pnl \
        .rollup(
            aggs=[agg.last("TradeTime=Timestamp"), agg.sum_(["PnL", "TradeSize"]), agg.avg("AvgTradePrice=TradePrice")],
            by=by,
            include_constituents=True,
        )
