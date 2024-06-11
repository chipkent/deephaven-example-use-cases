
#TODO: document and clean up

from deephaven import time_table, new_table, input_table, DynamicTableWriter
from deephaven.column import string_col
import deephaven.dtypes as dht
from deephaven.updateby import ema_time, emstd_time
from deephaven.plot import Figure, PlotStyle
from deephaven.plot.selectable_dataset import one_click

max_position_dollars = 10000.0
ema_decay_time = "PT00:02:00"

print("==============================================================================================================")
print("==== Create simulated trade and position tables.")
print("==============================================================================================================")

trades_writer = DynamicTableWriter({
    "Date": dht.string,
    "Timestamp": dht.Instant,
    "Sym": dht.string,
    "Price": dht.float64,
    "Size": dht.int64})
trades = trades_writer.table

positions = trades.view(["Sym", "Position=Size"]).sum_by("Sym")


print("==============================================================================================================")
print("==== Dynamic table of active securities.")
print("==============================================================================================================")

active_syms = input_table(init_table=new_table([string_col("Sym", ["AAPL", "GOOG", "BAC"])]))

ticks_bid_ask = db.live_table("FeedOS", "EquityQuoteL1") \
    .view(["Date", "Timestamp", "Sym = LocalCodeStr", "BidPrice=Bid", "BidSize", "AskPrice=Ask", "AskSize"]) \
    .where(["Date = today()", "BidSize > 0", "AskSize > 0"]) \
    .where_in(active_syms, "Sym")

print("==============================================================================================================")
print("==== Compute predictions.")
print("==============================================================================================================")

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

preds_start = preds.first_by("Sym").view(["Sym", "Timestamp"])
preds = preds.natural_join(preds_start, on="Sym", joins="TimestampFirst=Timestamp")

print("==============================================================================================================")
print("==== Plot predictions.")
print("==============================================================================================================")

preds_one_click = one_click(preds, by=["Sym"], require_all_filters=True)

preds_plot = Figure() \
    .plot_xy("BidPrice", t=preds_one_click, x="Timestamp", y="BidPrice") \
    .plot_xy("AskPrice", t=preds_one_click, x="Timestamp", y="AskPrice") \
    .plot_xy("MidPrice", t=preds_one_click, x="Timestamp", y="MidPrice") \
    .plot_xy("PredPrice", t=preds_one_click, x="Timestamp", y="PredPrice") \
    .plot_xy("PredBuy", t=preds_one_click, x="Timestamp", y="PredBuy") \
    .plot_xy("PredSell", t=preds_one_click, x="Timestamp", y="PredSell") \
    .show()

print("==============================================================================================================")
print("==== Generate orders.")
print("==============================================================================================================")

orders = preds.last_by(["Sym"]) \
    .where(f"Timestamp > TimestampFirst + '{ema_decay_time}'") \
    .natural_join(positions, on="Sym", joins="Position") \
    .update_view([
        "Position = replaceIfNull(Position, 0.0)",
        "PositionDollars = Position * MidPrice",
        "MaxPositionDollars = max_position_dollars",
        "BuyActive = PositionDollars < MaxPositionDollars",
        "SellActive = PositionDollars > -MaxPositionDollars",
    ])


print("==============================================================================================================")
print("==== Simulate trading.")
print("==============================================================================================================")

def simulate_1_lot(date, timestamp, sym, bid, ask, pred_buy, pred_sell, is_buy_active, is_sell_active) -> str:

    if is_buy_active and ask < pred_buy:
        trades_writer.write_row(date, timestamp, sym, ask, 100)
        return "BUY"

    if is_sell_active and bid > pred_sell:
        trades_writer.write_row(date, timestamp, sym, bid, -100)
        return "SELL"

    return "NO TRADE"

executions = orders \
    .snapshot_when(time_table("PT00:01:00"), stamp_cols="SnapTime=Timestamp") \
    .update("Size = simulate_1_lot(Date, Timestamp, Sym, BidPrice, AskPrice, PredBuy, PredSell, BuyActive, SellActive)")

print("==============================================================================================================")
print("==== Plot trade executions.")
print("==============================================================================================================")

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