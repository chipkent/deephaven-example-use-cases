""" Compute execution slippage - price movement after trade execution.

Slippage measures the quality of trade execution by analyzing how prices move
after a trade is executed. This helps answer:
- Did we execute at favorable prices?
- Are prices moving against us immediately after execution?
- Which securities have the best/worst execution quality?

Methodology:
1. For each trade, look ahead by the holding period (e.g., 1 minute)
2. Compare the future mid-price to the execution price
3. Calculate P&L: positive = favorable (price moved in our direction)
                  negative = unfavorable (price moved against us)
4. Aggregate by security to identify systematic execution issues

The holding_period parameter defines how far ahead to look (e.g., "PT1m" = 1 minute).
Shorter periods measure immediate market impact; longer periods measure sustained price trends.

Output (RollupTable):
- Hierarchical aggregation by USym → Expiry → Strike → Parity
- TradeTime: Timestamp of last trade
- PnL: Total profit/loss from price movement after trades
- TradeSize: Net position traded
- AvgTradePrice: Volume-weighted average execution price
- Constituents: Individual trades contributing to aggregates

Use cases:
- Identify securities with poor execution quality
- Detect market impact (large trades moving prices against you)
- Optimize trading strategies and timing
"""

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
