"""
Trading Simulation Worker

This worker script runs as a Deephaven persistent query launched by the PQ orchestrator.
It simulates a mean-reversion trading strategy using EMA-based predictions on replayed market data.

Key Responsibilities:
    - Receives replayed market data from FeedOS/EquityQuoteL1
    - Computes EMA-based buy/sell thresholds for assigned symbols
    - Simulates trades based on predicted price bands and position limits
    - Manages positions and closes them before market close
    - Writes results to partitioned user tables for analysis

Required Environment Variables:
    - SIMULATION_NAME: Unique identifier for the simulation run
    - SIMULATION_DATE: Date being simulated
    - QUERY_NAME: Full persistent query name
    - OUTPUT_NAMESPACE: Namespace for output user tables
    - PARTITION_ID: Partition ID (0 to NUM_PARTITIONS-1)
    - NUM_PARTITIONS: Total number of partitions
    - MAX_POSITION_DOLLARS: Maximum position size in dollars
    - EMA_DECAY_TIME: EMA decay time window (ISO 8601 duration)
    - LOT_SIZE: Number of shares per trade

Output Tables:
    - TradingSimTrades: Individual trade executions
    - TradingSimPositions: Current positions by symbol
    - TradingSimPnl: Profit and loss by symbol
    - TradingSimPreds: EMA predictions and thresholds
    - TradingSimOrders: Trading signals and decisions
    - TradingSimExecutions: Periodic snapshots with action codes
    - TradingSimSummary: Aggregate trade statistics
"""

import os
import time
from deephaven import new_table, DynamicTableWriter, time_table, agg
from deephaven.column import string_col, double_col
import deephaven.dtypes as dht
from deephaven.updateby import ema_time, emstd_time, cum_min
from deephaven.time import dh_today, dh_now, to_j_instant

simulation_name = os.getenv("SIMULATION_NAME")
simulation_date = os.getenv("SIMULATION_DATE")
query_name = os.getenv("QUERY_NAME")
output_namespace = os.getenv("OUTPUT_NAMESPACE")
partition_id = int(os.getenv("PARTITION_ID"))
num_partitions = int(os.getenv("NUM_PARTITIONS"))
max_position_dollars = float(os.getenv("MAX_POSITION_DOLLARS"))
ema_decay_time = os.getenv("EMA_DECAY_TIME")
lot_size = int(os.getenv("LOT_SIZE"))

# Market time constants
market_close_time = to_j_instant(f"{dh_today()}T16:00:00 ET")
close_out_time = to_j_instant(f"{dh_today()}T15:50:00 ET")  # Stop trading 10 min before close

print(f"[INFO] Trading Simulation Worker Started")
print(f"[INFO] Simulation Name: {simulation_name}")
print(f"[INFO] Simulation Date: {simulation_date}")
print(f"[INFO] dh_today(): {dh_today()}")
print(f"[INFO] Partition ID: {partition_id}/{num_partitions}")
print(f"[INFO] Max Position: ${max_position_dollars}")
print(f"[INFO] EMA Decay Time: {ema_decay_time}")
print(f"[INFO] Lot Size: {lot_size}")

############################################################################################################
# Define stock universe and partition symbols across partitions
############################################################################################################

all_symbols = new_table([
    string_col("Sym", ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "BAC", "JPM", "WFC"]),
    double_col("MaxPositionDollars", [max_position_dollars] * 10)
])

my_symbols = all_symbols.where(f"Sym.hashCode() % {num_partitions} == {partition_id}")

print(f"[INFO] This partition handles {my_symbols.size} symbols")

############################################################################################################
# Create simulated trade and position tables
############################################################################################################

trades_writer = DynamicTableWriter({
    "Date": dht.string,
    "Timestamp": dht.Instant,
    "Sym": dht.string,
    "Price": dht.float64,
    "Size": dht.int64,
    "PartitionID": dht.int32
})
trades = trades_writer.table

positions = trades.view(["Sym", "Position=Size"]).sum_by("Sym")

############################################################################################################
# Get replay market data
############################################################################################################

trading_date = dh_today()

ticks_bid_ask = db.live_table("FeedOS", "EquityQuoteL1") \
    .where(["Date = trading_date", "BidSize > 0", "AskSize > 0"]) \
    .view(["Date", "Timestamp", "Sym = LocalCodeStr", "BidPrice=Bid", "BidSize", "AskPrice=Ask", "AskSize"]) \
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
        "TradingActive = Timestamp < close_out_time",
    ])

############################################################################################################
# Simulate trading
############################################################################################################

def simulate_trade(date, timestamp, sym, bid, ask, pred_buy, pred_sell, is_buy_active, is_sell_active, trading_active, position) -> str:
    """
    Stateless trade simulation function.
    
    Args:
        date: Trading date (string)
        timestamp: Current timestamp (Instant)
        sym: Stock symbol
        bid: Current bid price
        ask: Current ask price
        pred_buy: Predicted buy threshold (lower band)
        pred_sell: Predicted sell threshold (upper band)
        is_buy_active: Boolean, true if position allows buying
        is_sell_active: Boolean, true if position allows selling
        trading_active: Boolean, true if normal trading is allowed (before close-out)
        position: Current position size for this symbol
    
    Trading Logic:
        When trading is active:
            - Buy when position allows and ask < pred_buy (market below lower threshold)
            - Sell when position allows and bid > pred_sell (market above upper threshold)
        
        When trading is not active (close-out period):
            - Close any non-zero positions by crossing the market
        
    Returns:
        Action code (str): BUY, SELL, CLOSE_LONG, CLOSE_SHORT, TRADING_OFF, NO_TRADE
            - BUY: Bought at ask (normal trading)
            - SELL: Sold at bid (normal trading)
            - CLOSE_LONG: Closed long position during close-out
            - CLOSE_SHORT: Closed short position during close-out
            - TRADING_OFF: Close-out period but no position to close
            - NO_TRADE: Trading active but criteria not met
    """
    # Close-out period: close any open positions
    if not trading_active:
        if position > 0:
            # Long position - sell at bid to close
            trades_writer.write_row(date, timestamp, sym, bid, -position, partition_id)
            return "CLOSE_LONG"
        elif position < 0:
            # Short position - buy at ask to close
            trades_writer.write_row(date, timestamp, sym, ask, -position, partition_id)
            return "CLOSE_SHORT"
        return "TRADING_OFF"
    
    # Normal trading period
    if is_buy_active and ask < pred_buy:
        trades_writer.write_row(date, timestamp, sym, ask, lot_size, partition_id)
        return "BUY"
    
    if is_sell_active and bid > pred_sell:
        trades_writer.write_row(date, timestamp, sym, bid, -lot_size, partition_id)
        return "SELL"
    
    return "NO_TRADE"

# Execute trading decisions every 10 seconds
executions = orders \
    .snapshot_when(time_table("PT00:00:10"), stamp_cols="SnapTime=Timestamp") \
    .update("Action = simulate_trade(Date, Timestamp, Sym, BidPrice, AskPrice, PredBuy, PredSell, BuyActive, SellActive, TradingActive, Position)")

############################################################################################################
# Summary statistics
############################################################################################################

trade_summary = trades.agg_by([
    agg.count_("TradeCount"),
    agg.sum_("TotalShares=Size"),
], by=["Sym"])

############################################################################################################
# User table persistence
############################################################################################################

def write_partitioned_tables():
    """
    Write all simulation results to partitioned user tables.
    Uses Deephaven Enterprise user table API with multi-column partitioning.
    """
    partition_updates = [
        "SimulationName = simulation_name",
        "PartitionID = partition_id",
        "Date = simulation_date"
    ]
    
    tables_to_write = [
        (trades, "TradingSimTrades"),
        (positions, "TradingSimPositions"),
        (pnl, "TradingSimPnl"),
        (preds, "TradingSimPreds"),
        (orders, "TradingSimOrders"),
        (executions, "TradingSimExecutions"),
        (trade_summary, "TradingSimSummary")
    ]
    
    print(f"[INFO] Writing user tables for simulation: {simulation_name}")
    
    # Partition key: SimulationName/Date/PartitionID
    partition_key = f"{simulation_name}/{simulation_date}/{partition_id}"
    
    for table, table_name in tables_to_write:
        try:
            table_with_partitions = table.update_view(partition_updates)
            db.add_table_partition(output_namespace, table_name, partition_key, table_with_partitions)
            print(f"[INFO] Wrote {table_name}")
        except Exception as e:
            print(f"[ERROR] Failed to write {table_name}: {e}")
    
    print(f"[INFO] Successfully wrote all tables to user tables")

############################################################################################################
# Initialization complete
############################################################################################################

print(f"[INFO] Trading simulation running for {my_symbols.size} symbols")
print(f"[INFO] Partition {partition_id} will write results to partitioned tables")
print(f"[INFO] Trading Simulation Partition Initialized Successfully")

############################################################################################################
# Monitor until market close and exit
############################################################################################################

from deephaven_enterprise.client.session_manager import SessionManager

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
        
        write_partitioned_tables()
        
        print(f"[INFO] Trading Simulation Worker Completed Successfully")
        
        sm = SessionManager()
        sm.controller_client.stop_and_wait(__PERSISTENT_QUERY_SERIAL_NUMBER)
        print(f"[INFO] Persistent query stopped successfully")
        break

    # Sleep to avoid busy-waiting (real-time sleep, not simulation time)
    time.sleep(check_interval)

print(f"[INFO] Trading Simulation Worker Completed Successfully")
