# Trading Simulation Example

A market maker simulation based on mean-reversion strategy, designed to backtest trading algorithms across historical data using the replay orchestration framework.

## Overview

This example implements a complete trading simulation based on [`examples/finance/simulated_market_maker`](../../finance/simulated_market_maker). It demonstrates how to run large-scale backtests by partitioning stocks across multiple partitions and processing historical dates in parallel.

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

**Note**: This example uses 10 hardcoded symbols (AAPL, GOOG, MSFT, etc.) for demonstration. To scale to larger universes like SP500, modify the `all_symbols` table in `trading_simulation.py`.

### Configuration

Key configuration settings:

```yaml
execution:
  num_partitions: 2            # 2 partitions per date (each processes a subset of stocks)
  max_concurrent_sessions: 10  # Max total sessions running concurrently
  
replay:
  heap_size_gb: 16.0           # RAM allocated per session
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

**2. Clean existing tables** - Run [`manage_user_tables.py`](manage_user_tables.py) in the Deephaven console and call `delete_all_tables()` to remove any previous simulation data.

**3. Run** - From the `replay_orchestration` directory:
```bash
replay-orchestrator --config trading_simulation/config.yaml
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

## Querying Results

Use [`manage_user_tables.py`](manage_user_tables.py) in the Deephaven console to access results:

```python
# List tables and row counts
list_tables()

# Get tables for analysis
trades = get_table("TradingSimTrades")
pnl = get_table("TradingSimPnl")

# Query the data
trades.where("Sym = `AAPL`").tail(100)
pnl.view(["Date", "Sym", "PnL"])
```

To delete all simulation data: `delete_all_tables()`

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

1. **Change stock universe**: Modify the `all_symbols` table in `trading_simulation.py`
2. **Adjust strategy parameters**: Update `MAX_POSITION_DOLLARS`, `EMA_DECAY_TIME`, `LOT_SIZE` in `config.yaml`
3. **Scale partitions**: Increase `num_partitions` to process more symbols in parallel
4. **Adjust date range**: Modify `dates.start` and `dates.end` in `config.yaml`
5. **Speed up replay**: Increase `replay_speed` for faster backtesting (currently 100x)
