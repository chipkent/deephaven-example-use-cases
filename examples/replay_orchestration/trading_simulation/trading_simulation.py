import os
import sys
import time
from deephaven import new_table, DynamicTableWriter, time_table, agg
from deephaven.column import string_col, double_col
import deephaven.dtypes as dht
from deephaven.updateby import ema_time, emstd_time, cum_min
from deephaven.time import dh_today, dh_now, to_j_instant

simulation_name = os.getenv("SIMULATION_NAME")
simulation_date = os.getenv("SIMULATION_DATE")
worker_id = int(os.getenv("WORKER_ID", "0"))
num_workers = int(os.getenv("NUM_WORKERS", "1"))
max_position_dollars = float(os.getenv("MAX_POSITION_DOLLARS", "10000"))
ema_decay_time = os.getenv("EMA_DECAY_TIME", "PT00:01:00")
lot_size = int(os.getenv("LOT_SIZE", "100"))

print(f"[INFO] Trading Simulation Worker Started")
print(f"[INFO] Simulation Name: {simulation_name}")
print(f"[INFO] Simulation Date: {simulation_date}")
print(f"[INFO] dh_today(): {dh_today()}")
print(f"[INFO] Worker ID: {worker_id}/{num_workers}")
print(f"[INFO] Max Position: ${max_position_dollars}")
print(f"[INFO] EMA Decay Time: {ema_decay_time}")
print(f"[INFO] Lot Size: {lot_size}")

############################################################################################################
# Define stock universe and partition by worker
############################################################################################################

all_symbols = new_table([
    string_col("Sym", ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "BAC", "JPM", "WFC"]),
    double_col("MaxPositionDollars", [max_position_dollars] * 10)
])

my_symbols = all_symbols.where(f"(int)(hashCode(Sym)) % {num_workers} == {worker_id}")
symbol_list = [row["Sym"] for row in my_symbols.to_dict()["Sym"]]

print(f"[INFO] This worker handles {len(symbol_list)} symbols: {', '.join(symbol_list)}")

############################################################################################################
# Create simulated trade and position tables
############################################################################################################

trades_writer = DynamicTableWriter({
    "Date": dht.string,
    "Timestamp": dht.Instant,
    "Sym": dht.string,
    "Price": dht.float64,
    "Size": dht.int64,
    "WorkerID": dht.int32
})
trades = trades_writer.table

positions = trades.view(["Sym", "Position=Size"]).sum_by("Sym")

############################################################################################################
# Get replay market data
############################################################################################################

ticks_bid_ask = db.live_table("FeedOS", "EquityQuoteL1") \
    .view(["Date", "Timestamp", "Sym = LocalCodeStr", "BidPrice=Bid", "BidSize", "AskPrice=Ask", "AskSize"]) \
    .where(["Date = today()", "BidSize > 0", "AskSize > 0"]) \
    .where_in(my_symbols, "Sym")

############################################################################################################
# Calculate PnL
############################################################################################################

pnl = trades \
    .natural_join(ticks_bid_ask, on="Sym", joins=["BidPrice", "AskPrice"]) \
    .update("PnL = Size * (Price - 0.5 * (BidPrice + AskPrice))") \
    .view(["Sym", "PnL"]).sum_by("Sym")

############################################################################################################
# Compute predictions using EMA
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
# Generate orders
############################################################################################################

orders = preds.last_by(["Sym"]) \
    .where(f"Timestamp > TimestampFirst + '{ema_decay_time}'") \
    .natural_join(positions, on="Sym", joins="Position") \
    .natural_join(my_symbols, on="Sym", joins="MaxPositionDollars") \
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

def simulate_trade(date, timestamp, sym, bid, ask, pred_buy, pred_sell, is_buy_active, is_sell_active) -> str:
    """
    Simulate a trade based on market conditions and predictions.
    
    Buy when position allows and ask < pred_buy (market price below lower threshold)
    Sell when position allows and bid > pred_sell (market price above upper threshold)
    """
    
    if is_buy_active and ask < pred_buy:
        trades_writer.write_row(date, timestamp, sym, ask, lot_size, worker_id)
        return "BUY"
    
    if is_sell_active and bid > pred_sell:
        trades_writer.write_row(date, timestamp, sym, bid, -lot_size, worker_id)
        return "SELL"
    
    return "NO TRADE"

executions = orders \
    .snapshot_when(time_table("PT00:00:10"), stamp_cols="SnapTime=Timestamp") \
    .update("Action = simulate_trade(Date, Timestamp, Sym, BidPrice, AskPrice, PredBuy, PredSell, BuyActive, SellActive)")

############################################################################################################
# Summary statistics
############################################################################################################

trade_summary = trades.agg_by([
    agg.count_("TradeCount"),
    agg.sum_("TotalShares=Size"),
], by=["Sym"])

print(f"[INFO] Trading simulation running for {len(symbol_list)} symbols")
print(f"[INFO] Worker {worker_id} will write results to partitioned tables")
print(f"[INFO] Trading Simulation Worker Initialized Successfully")

############################################################################################################
# Monitor until market close and exit
############################################################################################################

from deephaven_enterprise.client.session_manager import SessionManager
import time

print(f"[INFO] Monitoring until market close...")

# Monitor dh_now() and exit at market close (16:00:00 in scheduler)
end_time = to_j_instant(f"{dh_today()}T16:00:00 ET")
check_interval = 10  # Check every 10 seconds

# Track start times for speedup calculation
start_simulation_time = dh_now()
start_real_time = time.time()

while True:
    current_time = dh_now()
    current_real_time = time.time()
    
    # Calculate actual speedup
    elapsed_sim_nanos = (current_time.toEpochMilli() - start_simulation_time.toEpochMilli()) * 1_000_000
    elapsed_real_nanos = (current_real_time - start_real_time) * 1_000_000_000
    actual_speedup = elapsed_sim_nanos / elapsed_real_nanos if elapsed_real_nanos > 0 else 0.0
    
    print(f"[INFO] Current time: {current_time}, End time: {end_time}, Actual speedup: {actual_speedup:.2f}x")
    
    if current_time >= end_time:
        print(f"[INFO] Market close reached at {current_time}, exiting...")
        print(f"[INFO] Trading Simulation Worker Completed Successfully")
        sm=SessionManager()
        sm.controller_client.stop_and_wait(__PERSISTENT_QUERY_SERIAL_NUMBER)
        break

    # Sleep to avoid busy-waiting (real-time sleep, not simulation time)
    time.sleep(check_interval)

print(f"[INFO] Trading Simulation Worker Completed Successfully")
