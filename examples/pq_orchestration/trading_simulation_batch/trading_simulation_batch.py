"""
Trading Simulation Worker - Batch Mode

This worker script runs as a Deephaven batch persistent query launched by the PQ orchestrator.
It performs vectorized trading simulation on historical market data using static table operations.

Key Responsibilities:
    - Processes historical market data for a specific date and partition
    - Computes EMA-based buy/sell predictions vectorized across all ticks
    - Simulates trades using group_by + UDF for position tracking per symbol
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
    - TradingSimPositions: Final positions by symbol
    - TradingSimPnl: Profit and loss by symbol
    - TradingSimPreds: EMA predictions and thresholds
    - TradingSimSummary: Aggregate trade statistics
"""

import os
from deephaven import agg
from deephaven.column import string_col, double_col
from deephaven.time import dh_today, to_j_instant
from deephaven.updateby import ema_time, emstd_time
from numpy import typing as npt
import numpy as np
import numba

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
close_out_time = to_j_instant(f"{simulation_date}T15:50:00 ET")

print(f"[INFO] Trading Simulation Batch Worker Started")
print(f"[INFO] Simulation Name: {simulation_name}")
print(f"[INFO] Simulation Date: {simulation_date}")
print(f"[INFO] Partition ID: {partition_id}/{num_partitions}")
print(f"[INFO] Max Position: ${max_position_dollars}")
print(f"[INFO] EMA Decay Time: {ema_decay_time}")
print(f"[INFO] Lot Size: {lot_size}")

############################################################################################################
# Define stock universe and partition symbols across partitions
############################################################################################################

from deephaven import new_table

all_symbols = new_table([
    string_col("Sym", ["AAPL", "GOOG", "MSFT", "AMZN", "TSLA", "META", "NVDA", "BAC", "JPM", "WFC"]),
    double_col("MaxPositionDollars", [max_position_dollars] * 10)
])

my_symbols = all_symbols.where(f"Sym.hashCode() % {num_partitions} == {partition_id}")

print(f"[INFO] This partition handles {my_symbols.size} symbols")
if my_symbols.size > 0:
    print(f"[INFO] Symbols for partition {partition_id}: {my_symbols.view(['Sym']).to_string()}")
else:
    print(f"[WARNING] Partition {partition_id} has ZERO symbols assigned - all tables will be empty!")

############################################################################################################
# Get historical market data
############################################################################################################

ticks_raw = db.historical_table("FeedOS", "EquityQuoteL1") \
    .where([f"Date == simulation_date", "BidSize > 0", "AskSize > 0"]) \
    .view(["Date", "Timestamp", "Sym = LocalCodeStr", "BidPrice=Bid", "BidSize", "AskPrice=Ask", "AskSize"])

print(f"[INFO] Raw FeedOS data: {ticks_raw.size} ticks")
if ticks_raw.size > 0:
    all_symbols_in_feedos = ticks_raw.select_distinct("Sym").sort("Sym")
    print(f"[INFO] All symbols in FeedOS for {simulation_date}: {all_symbols_in_feedos.view(['Sym']).to_string()}")
else:
    print(f"[WARNING] No FeedOS data found for {simulation_date}!")

ticks_bid_ask = ticks_raw.where_in(my_symbols, "Sym").sort("Timestamp")

print(f"[INFO] Loaded historical market data: {ticks_bid_ask.size} ticks")
if ticks_bid_ask.size == 0:
    print(f"[WARNING] No market data found for partition {partition_id} symbols!")
else:
    symbols_in_data = ticks_bid_ask.select_distinct("Sym").sort("Sym")
    print(f"[INFO] Symbols present in loaded data: {symbols_in_data.view(['Sym']).to_string()}")

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
    ])

print(f"[INFO] Computed EMA predictions")

############################################################################################################
# Vectorized trading logic using group_by + UDF
############################################################################################################

@numba.guvectorize([(numba.bool_[:], numba.float64[:], numba.float64[:], numba.float64[:], numba.float64[:], numba.float64[:], numba.int64[:])], 
                   "(n),(n),(n),(n),(n),(n)->(n)")
def compute_trades(close_only, pred_buy, pred_sell, bid, ask, mid, trade_sizes):
    """
    Compute trade sizes for all ticks of one symbol using numba for performance.
    
    This function processes ticks sequentially to maintain position state.
    Sets trade_sizes array (positive=buy, negative=sell, 0=no trade).
    
    Args:
        close_only: Boolean array indicating close-out period
        pred_buy: Buy threshold prices (predicted price - std dev)
        pred_sell: Sell threshold prices (predicted price + std dev)
        bid: Bid prices
        ask: Ask prices
        mid: Mid prices for position valuation
        trade_sizes: Output array to populate with trade sizes
    """
    n = close_only.shape[0]
    position = 0
    
    for i in range(n):
        position_value = position * mid[i]
        
        # Close-out period: close any positions
        if close_only[i]:
            if position != 0:
                trade_sizes[i] = -position
                position = 0
            else:
                trade_sizes[i] = 0
            continue
        
        # Normal trading: check position limits before trading
        if ask[i] < pred_buy[i] and position_value < max_position_dollars:
            trade_sizes[i] = lot_size
            position += lot_size
        elif bid[i] > pred_sell[i] and position_value > -max_position_dollars:
            trade_sizes[i] = -lot_size
            position -= lot_size
        else:
            trade_sizes[i] = 0

print(f"[INFO] Generating trades using vectorized UDF")

# Generate trades using group_by + UDF
trades = preds \
    .update_view(["CloseOnly = Timestamp >= close_out_time"]) \
    .group_by(["Sym"]) \
    .update(["TradeSize = compute_trades(CloseOnly, PredBuy, PredSell, BidPrice, AskPrice, MidPrice)"]) \
    .ungroup() \
    .where("TradeSize != 0") \
    .update([
        "Price = TradeSize > 0 ? AskPrice : BidPrice",
        "Size = TradeSize",
        "Date = simulation_date",
        "PartitionID = partition_id"
    ]) \
    .view(["Date", "Timestamp", "Sym", "Price", "Size", "PartitionID"])

print(f"[INFO] Generated {trades.size} trades")

############################################################################################################
# Calculate positions and PnL
############################################################################################################

positions = trades.view(["Sym", "Position=Size"]).sum_by("Sym")

# Get final prices for PnL calculation
final_prices = ticks_bid_ask.last_by("Sym").view(["Sym", "FinalBid=BidPrice", "FinalAsk=AskPrice", "FinalMid=0.5*(BidPrice+AskPrice)"])

pnl = trades \
    .natural_join(final_prices, on="Sym", joins=["FinalMid"]) \
    .update("PnL = Size * (Price - FinalMid)") \
    .view(["Sym", "PnL"]).sum_by("Sym")

print(f"[INFO] Calculated positions and PnL")

############################################################################################################
# Summary statistics
############################################################################################################

trade_summary = trades.agg_by([
    agg.count_("TradeCount"),
    agg.sum_("TotalShares=Size"),
], by=["Sym"])

print(f"[INFO] Computed summary statistics")

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
        (preds.last_by("Sym"), "TradingSimPreds"),
        (trade_summary, "TradingSimSummary")
    ]
    
    print(f"[INFO] Writing user tables for simulation: {simulation_name}")
    
    # Partition key: SimulationName_Date_PartitionID
    partition_key = f"{simulation_name}_{simulation_date}_{partition_id}"
    print(f"[INFO] Using partition key: {partition_key}")
    
    for table, table_name in tables_to_write:
        try:
            table_with_partitions = table.update_view(partition_updates)
            row_count = table_with_partitions.size
            print(f"[INFO] Preparing to write {table_name}: {row_count} rows (partition_key={partition_key})")
            
            if row_count == 0:
                print(f"[WARNING] {table_name} has ZERO rows for partition_key={partition_key}!")
            
            # Try to create table schema - may race with other partitions
            try:
                db.add_partitioned_table_schema(output_namespace, table_name, "PartitionKey", table_with_partitions)
                print(f"[INFO] Created table schema: {table_name}")
            except Exception as schema_error:
                # Schema may already exist from another partition - this is OK
                if "table already exists" in str(schema_error).lower() or "already exists" in str(schema_error).lower():
                    print(f"[INFO] Table schema already exists: {table_name} (another partition created it)")
                else:
                    raise
            
            print(f"[INFO] Calling add_table_partition(namespace='{output_namespace}', table_name='{table_name}', partition_key='{partition_key}', rows={row_count})")
            db.add_table_partition(output_namespace, table_name, partition_key, table_with_partitions)
            print(f"[INFO] Successfully wrote {table_name} with {row_count} rows to partition key: {partition_key}")
        except Exception as e:
            print(f"[ERROR] Failed to write {table_name}: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    print(f"[INFO] Successfully wrote all tables to user tables")

write_partitioned_tables()

print(f"[INFO] Trading Simulation Batch Worker Completed Successfully")
