# Description: This script simulates a simple stock market maker using a mean-reversion strategy.
#
# Trading Strategy:
# - Uses exponential moving average (EMA) and standard deviation to predict price movements
# - Buys when the ask price drops below (predicted price - 1 standard deviation)
# - Sells when the bid price rises above (predicted price + 1 standard deviation)
# - Manages risk through per-symbol position limits in dollars
#
# Requirements:
# - Deephaven Enterprise with FeedOS access for live market data
# - Can be adapted to Deephaven Community by replacing FeedOS with alternative data source
#
# Output Tables:
# - controls: User-editable table for symbol selection and risk limits
# - ticks_bid_ask: Live bid/ask quotes for monitored symbols
# - preds: Price predictions with EMA-based buy/sell thresholds
# - trades: Historical record of all simulated trades
# - positions: Current position (shares held) per symbol
# - pnl: Unrealized profit and loss per symbol
# - orders: Current trading signals and position status
# - executions: Snapshot of recent trade decisions

from deephaven import time_table, new_table, input_table, DynamicTableWriter
from deephaven.column import string_col, double_col
import deephaven.dtypes as dht
from deephaven.updateby import ema_time, emstd_time, cum_min
from deephaven.plot import Figure, PlotStyle
from deephaven.plot.selectable_dataset import one_click

ema_decay_time = "PT00:01:00"  # EMA decay time window: 1 minute

############################################################################################################
# Create user-modifiable strategy controls table.
############################################################################################################

controls = input_table(init_table=new_table([
    string_col("Sym", ["AAPL", "GOOG", "BAC"]),
    double_col("MaxPositionDollars", [10000.0, 10000.0, 100000.0])
]), key_cols=["Sym"])


############################################################################################################
# Create simulated trade and position tables.
#
# The trades table will be used to record simulated trades.
############################################################################################################

trades_writer = DynamicTableWriter({
    "Date": dht.string,
    "Timestamp": dht.Instant,
    "Sym": dht.string,
    "Price": dht.float64,
    "Size": dht.int64})
trades = trades_writer.table

positions = trades.view(["Sym", "Position=Size"]).sum_by("Sym")


############################################################################################################
# Get a price feed table
############################################################################################################

feedos_tables = db.catalog_table().where("Namespace=`FeedOS`")

ticks_bid_ask = db.live_table("FeedOS", "EquityQuoteL1") \
    .view(["Date", "Timestamp", "Sym = LocalCodeStr", "BidPrice=Bid", "BidSize", "AskPrice=Ask", "AskSize"]) \
    .where(["Date = today()", "BidSize > 0", "AskSize > 0"]) \
    .where_in(controls, "Sym")


############################################################################################################
# Calculate PnL
############################################################################################################

pnl = trades \
    .natural_join(ticks_bid_ask, on="Sym", joins=["BidPrice", "AskPrice"]) \
    .update("PnL = Size * (Price - 0.5 * (BidPrice + AskPrice))") \
    .view(["Sym", "PnL"]).sum_by("Sym")


############################################################################################################
# Compute predictions
############################################################################################################

preds = ticks_bid_ask \
    .update_view(["MidPrice=0.5*(BidPrice+AskPrice)"]) \
    .update_by([
        ema_time("Timestamp", ema_decay_time, ["PredPrice=MidPrice"]),
        emstd_time("Timestamp", ema_decay_time, ["PredSD=MidPrice"]),
    ], by="Sym") \
    .update_view([
        "PredBuy=PredPrice-PredSD",
        "PredSell=PredPrice+PredSD",
    ]) \
    .update_by([cum_min("TimestampFirst=Timestamp")], by="Sym")


############################################################################################################
# Plot predictions
############################################################################################################

preds_one_click = one_click(preds, by=["Sym"], require_all_filters=True)

preds_plot = Figure() \
    .plot_xy("BidPrice", t=preds_one_click, x="Timestamp", y="BidPrice") \
    .plot_xy("AskPrice", t=preds_one_click, x="Timestamp", y="AskPrice") \
    .plot_xy("MidPrice", t=preds_one_click, x="Timestamp", y="MidPrice") \
    .plot_xy("PredPrice", t=preds_one_click, x="Timestamp", y="PredPrice") \
    .plot_xy("PredBuy", t=preds_one_click, x="Timestamp", y="PredBuy") \
    .plot_xy("PredSell", t=preds_one_click, x="Timestamp", y="PredSell") \
    .show()


############################################################################################################
# Generate orders
############################################################################################################

orders = preds.last_by(["Sym"]) \
    .where(f"Timestamp > TimestampFirst + '{ema_decay_time}'") \
    .natural_join(positions, on="Sym", joins="Position") \
    .natural_join(controls, on="Sym", joins="MaxPositionDollars") \
    .update_view([
        "Position = replaceIfNull(Position, 0.0)",
        "PositionDollars = Position * MidPrice",
        "MaxPositionDollars = replaceIfNull(MaxPositionDollars, 0.0)",
        "BuyActive = PositionDollars < MaxPositionDollars",
        "SellActive = PositionDollars > -MaxPositionDollars",
    ])


############################################################################################################
# Simulate trading
############################################################################################################

def simulate_1_lot(date, timestamp, sym, bid, ask, pred_buy, pred_sell, is_buy_active, is_sell_active) -> str:
    """
    Simulate a trade of 1 lot (100 shares) based on market conditions and predictions.
    
    Buy Logic: When position limits allow and ask < pred_buy (market price below lower threshold)
    Sell Logic: When position limits allow and bid > pred_sell (market price above upper threshold)
    
    Args:
        date: Trading date
        timestamp: Current timestamp
        sym: Stock symbol
        bid: Current bid price
        ask: Current ask price
        pred_buy: Buy threshold (predicted price - 1 SD)
        pred_sell: Sell threshold (predicted price + 1 SD)
        is_buy_active: Whether buy orders are allowed (position limit check)
        is_sell_active: Whether sell orders are allowed (position limit check)
    
    Returns:
        Trade action: "BUY", "SELL", or "NO TRADE"
    """

    if is_buy_active and ask < pred_buy:
        trades_writer.write_row(date, timestamp, sym, ask, 100)
        return "BUY"

    if is_sell_active and bid > pred_sell:
        trades_writer.write_row(date, timestamp, sym, bid, -100)
        return "SELL"

    return "NO TRADE"

executions = orders \
    .snapshot_when(time_table("PT00:00:10"), stamp_cols="SnapTime=Timestamp") \
    .update("Size = simulate_1_lot(Date, Timestamp, Sym, BidPrice, AskPrice, PredBuy, PredSell, BuyActive, SellActive)")


############################################################################################################
# Plot trade executions
############################################################################################################

buys_one_click = one_click(trades.where("Size > 0"), by=["Sym"], require_all_filters=True)
sells_one_click = one_click(trades.where("Size < 0"), by=["Sym"], require_all_filters=True)

execution_plot = Figure() \
    .plot_xy("BidPrice", t=preds_one_click, x="Timestamp", y="BidPrice") \
    .plot_xy("AskPrice", t=preds_one_click, x="Timestamp", y="AskPrice") \
    .plot_xy("MidPrice", t=preds_one_click, x="Timestamp", y="MidPrice") \
    .plot_xy("PredPrice", t=preds_one_click, x="Timestamp", y="PredPrice") \
    .plot_xy("PredBuy", t=preds_one_click, x="Timestamp", y="PredBuy") \
    .plot_xy("PredSell", t=preds_one_click, x="Timestamp", y="PredSell") \
    .twin() \
    .axes(plot_style=PlotStyle.SCATTER) \
    .plot_xy("Buys", t=buys_one_click, x="Timestamp", y="Price") \
    .point(size=3) \
    .plot_xy("Sells", t=sells_one_click, x="Timestamp", y="Price") \
    .point(size=3) \
    .show()
