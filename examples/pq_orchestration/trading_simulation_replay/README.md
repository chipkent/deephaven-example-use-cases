# Trading Simulation Replay Example

A market maker simulation based on mean-reversion strategy, designed to backtest trading algorithms across historical data using the persistent query orchestration framework with realistic time-based data arrival.

## Overview

This example implements a complete trading simulation based on [`examples/finance/simulated_market_maker`](../../finance/simulated_market_maker). It demonstrates how to run large-scale backtests by partitioning stocks across multiple partitions and processing historical dates in parallel using replay mode (live data simulation).

**Batch alternative**: For vectorized processing of complete historical datasets without time simulation, see [`trading_simulation_batch/`](../trading_simulation_batch/).

## Implementation

### Strategy

Mean-reversion market making:

- Calculate exponential moving average (EMA) and standard deviation
- Buy when ask price drops below (predicted price - 1 SD)
- Sell when bid price rises above (predicted price + 1 SD)
- Manage risk through per-symbol position limits

### Data Partitioning

Each partition processes a subset of stocks:

```python
partition_id = int(os.getenv("PARTITION_ID"))      # 0-1 for this date
num_partitions = int(os.getenv("NUM_PARTITIONS"))  # 2 partitions per date

# Symbols are distributed using hash-based partitioning
my_symbols = all_symbols.where(f"Sym.hashCode() % {num_partitions} == {partition_id}")
```

**Note**: This example uses 10 hardcoded symbols (AAPL, GOOG, MSFT, etc.) for demonstration. To scale to larger universes like SP500, modify the `all_symbols` table in [`trading_simulation_replay.py`](trading_simulation_replay.py).

### Configuration

Key configuration settings:

```yaml
execution:
  num_partitions: 2            # 2 partitions per date (each processes a subset of stocks)
  max_concurrent_sessions: 10  # Max total sessions running concurrently
  heap_size_gb: 16.0           # RAM allocated per session
  script_language: "Python"
  
replay:
  init_timeout_minutes: 30
  replay_start: "09:30:00"
  replay_speed: 100.0          # 100x speed for faster backtesting
  
dates:
  start: "2024-01-01"
  end: "2024-12-31"
  weekdays_only: true          # 250 trading days
```

This creates **2 partitions per date × 250 days = 500 total replay sessions**.

**Note**: For larger-scale backtesting, increase `num_partitions` (e.g., to 10 for 2,500 sessions) and adjust `max_concurrent_sessions` based on your server capacity.

### Environment Variables

From config:

- `MAX_POSITION_DOLLARS`: "10000" - position limit per symbol
- `EMA_DECAY_TIME`: "PT00:01:00" - EMA window (1 minute)
- `LOT_SIZE`: "100" - shares per trade

Auto-generated:

- `SIMULATION_NAME`: Unique identifier for the simulation run
- `SIMULATION_DATE`: Date being simulated - also available via [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today)
- `PARTITION_ID`: Partition ID (0 to NUM_PARTITIONS-1) - this example uses it for stock partitioning
- `NUM_PARTITIONS`: Total partitions per date

## Prerequisites

See the [main README](../README.md) for setup instructions. This example requires:

- Deephaven Enterprise with FeedOS access for historical equity quote data

## Quick Start

**1. Configure** - Edit [`config.yaml`](config.yaml) to set date range and parameters. For testing, use a small date range (1-5 days).

**2. Clean existing tables** - Run the cleanup script to remove any previous simulation data:

```bash
pq-orchestrator --config trading_simulation_replay/cleanup.yaml
```

**3. Run** - From the `pq_orchestration` directory:

```bash
pq-orchestrator --config trading_simulation_replay/config.yaml
```

This creates 500 sessions (2 partitions × 250 trading days) and writes results to partitioned user tables in the `ExampleReplayTradingSim` namespace. Tables are auto-created on first write.

## Output Tables

Results are written to partitioned user tables in the namespace specified by `OUTPUT_NAMESPACE` (default: `"ExampleReplayTradingSim"`):

- **`TradingSimTrades`**: All executed trades with Date, Timestamp, Symbol, Price, Size, PartitionID
- **`TradingSimPositions`**: Current positions by symbol (cumulative shares held)
- **`TradingSimPnl`**: Profit and loss calculations per symbol
- **`TradingSimPreds`**: Price predictions with EMA-based buy/sell thresholds
- **`TradingSimOrders`**: Current trading signals and position status
- **`TradingSimExecutions`**: Periodic snapshots with action codes
- **`TradingSimSummary`**: Aggregated trade counts and total shares by symbol

All tables include partition columns: `SimulationName`, `PartitionID`, `Date`

## Analysis Tools

Two utility scripts are provided for working with simulation results in the Deephaven IDE console:

### [analyze_trading_results.py](analyze_trading_results.py)

Comprehensive quantitative analysis of simulation performance with professional risk metrics.

**Load the script:**

In the Deephaven IDE console, open the script file and execute it directly (use the "Run" button or Ctrl/Cmd+Enter).

**Quick start workflow:**

```python
# Your simulation name from config.yaml (line 1 of config.yaml)
sim_name = "trading_simulation_replay"

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

### [cleanup.py](cleanup.py) and [cleanup.yaml](cleanup.yaml)

Cleanup script and configuration for deleting all simulation tables.

**Usage:**

Run via the orchestrator to delete all tables in the simulation namespace:

```bash
pq-orchestrator --config trading_simulation_replay/cleanup.yaml
```

This will delete all 7 tables created by the replay simulation:

- TradingSimTrades
- TradingSimPositions
- TradingSimPnl
- TradingSimPreds
- TradingSimOrders
- TradingSimExecutions
- TradingSimSummary

## Expected Results

After completion, you'll have:

- Historical trades across a full year (250 trading days)
- 500 total sessions (2 partitions × 250 days)
- Trades partitioned by date and partition
- Daily PnL by symbol
- Position history
- Strategy performance metrics

All data will be queryable in Deephaven for analysis and visualization.

## Key Features

**Data Partitioning**: Symbols are distributed across partitions using hash-based partitioning:

```python
my_symbols = all_symbols.where(f"Sym.hashCode() % {num_partitions} == {partition_id}")
```

**Mean-Reversion Strategy**:

- Uses EMA and standard deviation to predict price movements
- Buys when ask < (predicted price - 1 SD)
- Sells when bid > (predicted price + 1 SD)
- Position limits prevent excessive exposure

**Replay Data**: Uses FeedOS `EquityQuoteL1` table filtered for:

- Current replay date via `dh_today()`
- Valid bid/ask quotes (size > 0)
- Symbols assigned to this partition

**Trade Execution**: Evaluates every 10 seconds via `snapshot_when()` and executes based on market conditions and position limits.

## Related Examples

See [`examples/finance/simulated_market_maker`](../../finance/simulated_market_maker) for the original market maker implementation this is based on.

## Customization

To adapt for your use case:

1. **Change stock universe**: Modify the `all_symbols` table in [`trading_simulation_replay.py`](trading_simulation_replay.py)
2. **Adjust strategy parameters**: Update `MAX_POSITION_DOLLARS`, `EMA_DECAY_TIME`, `LOT_SIZE` in [`config.yaml`](config.yaml)
3. **Scale partitions**: Increase `num_partitions` to process more symbols in parallel
4. **Adjust date range**: Modify `dates.start` and `dates.end` in [`config.yaml`](config.yaml)
5. **Speed up replay**: Increase `replay_speed` for faster backtesting (currently 100x)

## Files

- [`trading_simulation_replay.py`](trading_simulation_replay.py) - Replay worker script
- [`config.yaml`](config.yaml) - Replay configuration
- [`cleanup.py`](cleanup.py) - Cleanup script for deleting tables
- [`cleanup.yaml`](cleanup.yaml) - Cleanup orchestrator configuration
- [`analyze_trading_results.py`](analyze_trading_results.py) - Performance analysis tools
- [`README.md`](README.md) - This file
