# Trading Simulation Batch Example

Vectorized trading simulation using historical market data with PQ orchestration in batch mode.

## Overview

This example demonstrates how to perform large-scale trading strategy backtests using batch (RunAndDone) persistent queries. It processes historical market data across multiple dates and partitions using vectorized table operations.

## What It Does

Simulates a mean-reversion trading strategy on historical market data:
1. Loads historical equity quote data for assigned symbols
2. Computes EMA-based buy/sell predictions across all ticks
3. Generates trades using vectorized logic with position tracking
4. Closes positions before market close
5. Writes results to partitioned user tables

## Trading Logic

**Vectorized Implementation**:
- Uses `group_by().update(UDF).ungroup()` pattern
- UDF processes all ticks for each symbol sequentially
- Maintains position state across trades within the UDF
- Returns array of trade sizes for all ticks

**Strategy**:
- **Buy signal**: Ask price < (EMA - StdDev)
- **Sell signal**: Bid price > (EMA + StdDev)
- **Position limits**: $10,000 maximum position value
- **Close-out**: All positions closed at 3:50 PM ET

## Key Differences from Replay Version

**Data Access**:
```python
# Replay version
ticks = db.live_table("FeedOS", "EquityQuoteL1")

# Batch version
ticks = db.historical_table("FeedOS", "EquityQuoteL1") \
    .where(f"Date == '{simulation_date}'")
```

**Computation Model**:
- Replay: Incremental processing as data ticks in
- Batch: Vectorized processing of complete day's data

**Termination**:
- Replay: Manual `stop_and_wait()` call
- Batch: Automatic termination via RunAndDone

## Configuration

**Scale**: 2 partitions per date × 250 trading days = 500 sessions

**File**: [`config.yaml`](config.yaml)

Key settings:
```yaml
execution:
  num_partitions: 2
  max_concurrent_sessions: 10
  heap_size_gb: 16.0
  script_language: "Python"

batch:
  timeout_minutes: 60

dates:
  start: "2024-01-01"
  end: "2024-12-31"
  weekdays_only: true
```

## Prerequisites

See the [main README](../README.md) for setup instructions. This example requires:

- Deephaven Enterprise with FeedOS access for historical equity quote data

## Quick Start

**1. Configure** - Edit [`config.yaml`](config.yaml) to set date range and parameters. For testing, use a small date range (1-5 days).

**2. Clean existing tables** - Run [`manage_user_tables.py`](manage_user_tables.py) in the Deephaven console and call `delete_all_tables()` to remove any previous simulation data.

**3. Run** - Set environment variables and run from the `pq_orchestration` directory:

```bash
export DH_CONNECTION_URL="https://your-server:8000/iris/connection.json"
export DH_USERNAME="your_username"
export DH_PASSWORD="your_password"

pq-orchestrator --config trading_simulation_batch/config.yaml
```

This creates 500 sessions (2 partitions × 250 trading days) and writes results to partitioned user tables in the `ExampleBatchTradingSim` namespace. Tables are auto-created on first write. Monitor progress in the console output. Press Ctrl+C to gracefully stop.

## Output Tables

Results written to partitioned user tables in the namespace specified by `OUTPUT_NAMESPACE` (default: `"ExampleBatchTradingSim"`):

- **`TradingSimTrades`**: All executed trades with Date, Timestamp, Symbol, Price, Size, PartitionID
- **`TradingSimPositions`**: Final positions by symbol (cumulative shares held)
- **`TradingSimPnl`**: Profit and loss calculations per symbol
- **`TradingSimPreds`**: Price predictions with EMA-based buy/sell thresholds (last values per symbol)
- **`TradingSimSummary`**: Aggregated trade counts and total shares by symbol

**Note**: Batch mode creates 5 tables (no TradingSimOrders or TradingSimExecutions) since it uses vectorized processing without incremental order tracking.

All tables include partition columns: `SimulationName`, `PartitionID`, `Date`

## Vectorized Trading Implementation

The core trading logic uses a **[numba](https://numba.pydata.org/)-optimized UDF** for high performance:

```python
import numba

@numba.guvectorize([(numba.bool_[:], numba.float64[:], numba.float64[:], 
                     numba.float64[:], numba.float64[:], numba.float64[:], 
                     numba.int64[:])], 
                   "(n),(n),(n),(n),(n),(n)->(n)")
def compute_trades(close_only, pred_buy, pred_sell, bid, ask, mid, trade_sizes):
    """Compute trade sizes for all ticks of one symbol using numba for performance."""
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
```

Applied via group_by with pre-computed close-out flag:
```python
trades = preds \
    .update_view(["CloseOnly = Timestamp >= close_out_time"]) \
    .group_by(["Sym"]) \
    .update(["TradeSize = compute_trades(CloseOnly, PredBuy, PredSell, BidPrice, AskPrice, MidPrice)"]) \
    .ungroup() \
    .where("TradeSize != 0")
```

**Performance optimizations:**
- **Numba JIT compilation**: [`@numba.guvectorize`](https://numba.pydata.org/numba-doc/latest/user/vectorize.html#the-guvectorize-decorator) compiles to optimized machine code
- **Pre-computed close flag**: Boolean array avoids repeated timestamp comparisons
- **Vectorized types**: Explicit type signatures enable SIMD optimizations

See [Deephaven's numba guide](https://deephaven.io/core/docs/how-to-guides/use-numba/) for more details on using numba with Deephaven.

## Performance Notes

- Processes entire day's data in one batch
- Memory requirements scale with data volume
- Increase `heap_size_gb` for larger symbol universes or higher frequency data

## Analysis Tools

Two utility scripts are provided for working with simulation results in the Deephaven IDE console:

### [analyze_trading_results.py](analyze_trading_results.py)

Comprehensive quantitative analysis of simulation performance with professional risk metrics.

**Load the script:**

In the Deephaven IDE console, open the script file and execute it directly (use the "Run" button or Ctrl/Cmd+Enter).

**Quick start workflow:**

```python
# Your simulation name from config.yaml
sim_name = "trading_simulation_batch"

# Step 1: Get high-level overview
summary = get_summary(sim_name)
winners = summary["top_performers"]      # Top 5 stocks by P&L
losers = summary["bottom_performers"]    # Bottom 5 stocks by P&L

# Step 2: Analyze overall performance
pnl = analyze_pnl(sim_name)
overall = pnl["overall"]                 # Sharpe ratio, max drawdown, win rate
equity_curve = pnl["by_date"]            # Cumulative P&L over time

# Step 3: Investigate specific stocks
aapl = analyze_by_symbol(sim_name, "AAPL")
aapl_stats = aapl["stats"]               # Performance summary for AAPL
```

**Available functions:**

- `get_summary(sim_name)` - Start here! Overview with best/worst performers
- `analyze_pnl(sim_name)` - P&L metrics: Sharpe ratio, max drawdown, win rate
- `analyze_trades(sim_name)` - Trade statistics, turnover, buy/sell breakdown
- `analyze_by_symbol(sim_name, sym)` - Deep dive on a specific stock
- `analyze_by_date(sim_name, date)` - Analyze a specific trading day
- `analyze_positions(sim_name)` - Position sizing and distribution
- `analyze_executions(sim_name)` - Trading signal patterns

**Key metrics explained:**

- **Sharpe Ratio**: Risk-adjusted return (>1 is good, >2 is excellent)
- **Max Drawdown**: Worst peak-to-trough decline (more negative = worse)
- **Win Rate**: Percentage of profitable days (0.5 = 50%)
- **Turnover**: Total dollar value traded (for transaction cost estimation)

All functions return dictionaries of Deephaven tables that automatically display in the UI when assigned to variables.

### [manage_user_tables.py](manage_user_tables.py)

Table management utilities for accessing and cleaning simulation data.

**Load the script:**

In the Deephaven IDE console, open the script file and execute it directly (use the "Run" button or Ctrl/Cmd+Enter).

**Common operations:**

```python
# List all simulation tables with row counts
list_tables()

# Get a table for querying
trades = get_table("TradingSimTrades")
pnl = get_table("TradingSimPnl")

# Query the data
trades.where("Sym = `AAPL`").tail(100)
pnl.view(["Date", "Sym", "PnL"])

# Delete specific table
delete_table("TradingSimTrades")

# Delete all simulation data (use before new runs)
delete_all_tables()
```

**Available functions:**

- `list_tables()` - Show all tables in namespace with row counts
- `get_table(name)` - Retrieve a table for analysis
- `delete_table(name)` - Delete a specific table
- `delete_all_tables()` - Delete all simulation tables (prompts for confirmation)

## Expected Results

After completion, you'll have:

- Historical trades across a full year (250 trading days)
- 500 total sessions (2 partitions × 250 days)
- Trades partitioned by date and partition
- Daily PnL by symbol
- Position history
- Strategy performance metrics

All data will be queryable in Deephaven for analysis and visualization.

## Files

- [`trading_simulation_batch.py`](trading_simulation_batch.py) - Batch worker script
- [`config.yaml`](config.yaml) - Batch configuration
- [`manage_user_tables.py`](manage_user_tables.py) - Table management utilities
- [`analyze_trading_results.py`](analyze_trading_results.py) - Performance analysis tools
- [`README.md`](README.md) - This file
