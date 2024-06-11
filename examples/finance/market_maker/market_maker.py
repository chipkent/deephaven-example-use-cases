# Description: This script simulates a simple stock market maker.
# It uses an exponential moving average (EMA) to predict the price of a stock and make trades based on the prediction.
# It requires Deephaven Enterprise to run but can be addapted to Deephaven Community.

from deephaven import time_table, new_table, input_table, DynamicTableWriter
from deephaven.column import string_col, double_col
import deephaven.dtypes as dht
from deephaven.updateby import ema_time, emstd_time
from deephaven.plot import Figure, PlotStyle
from deephaven.plot.selectable_dataset import one_click

ema_decay_time = "PT00:01:00"

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
    ])

_preds_start = preds.first_by("Sym").view(["Sym", "Timestamp"])
preds = preds.natural_join(_preds_start, on="Sym", joins="TimestampFirst=Timestamp")


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
    """ Simulate a trade of 1 lot based on the current state of the market and predictions. """

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

buys = trades.where("Size > 0")
sells = trades.where("Size < 0")

buys_one_click = one_click(buys, by=["Sym"], require_all_filters=True)
sells_one_click = one_click(sells, by=["Sym"], require_all_filters=True)

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