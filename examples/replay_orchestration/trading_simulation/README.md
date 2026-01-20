# Trading Simulation Example

A market maker simulation based on mean-reversion strategy, designed to backtest trading algorithms across historical data using the replay orchestration framework.

## Overview

This example implements a complete trading simulation based on [`examples/finance/simulated_market_maker`](../../finance/simulated_market_maker). It demonstrates how to run large-scale backtests by partitioning stocks across multiple workers and processing historical dates in parallel.

## Implementation

### Strategy

Mean-reversion market making:

- Calculate exponential moving average (EMA) and standard deviation
- Buy when ask price drops below (predicted price - 1 SD)
- Sell when bid price rises above (predicted price + 1 SD)
- Manage risk through per-symbol position limits

### Worker Partitioning

Each worker processes a subset of stocks:

```python
worker_id = int(os.getenv("WORKER_ID"))      # 0-9 for this date
num_workers = int(os.getenv("NUM_WORKERS"))  # 10 workers per date

# Symbols are distributed using hash-based partitioning
my_symbols = all_symbols.where(f"Sym.hashCode() % {num_workers} == {worker_id}")
```

**Note**: This example uses 10 hardcoded symbols (AAPL, GOOG, MSFT, etc.) for demonstration. To scale to larger universes like SP500, modify the `all_symbols` table in `trading_simulation.py`.

### Configuration

This example is configured for large-scale backtesting:

```yaml
execution:
  num_workers: 10              # 10 workers per date (each processes a subset of stocks)
  max_concurrent_sessions: 20  # Max total sessions running concurrently
  
replay:
  heap_size_gb: 16.0           # More RAM for trading simulation
  replay_speed: 100.0          # 100x speed for faster backtesting
  
dates:
  start: "2024-01-01"
  end: "2024-12-31"
  weekdays_only: true          # 250 trading days
```

This creates **10 workers per date × 250 days = 2,500 total replay sessions**.

### Environment Variables

From config:

- `MAX_POSITION_DOLLARS`: "10000" - position limit per symbol
- `EMA_DECAY_TIME`: "PT00:01:00" - EMA window (1 minute)
- `LOT_SIZE`: "100" - shares per trade

Auto-generated:

- `SIMULATION_NAME`: Unique identifier for the simulation run
- `SIMULATION_DATE`: Date being simulated - also available via [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today)
- `WORKER_ID`: Worker partition ID (0 to NUM_WORKERS-1) - this example uses it for stock partitioning
- `NUM_WORKERS`: Total workers per date

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

This creates 2,500 sessions (10 workers × 250 trading days) and writes results to partitioned user tables in the `ExampleReplayTradingSim` namespace. Tables are auto-created on first write.

## Output Tables

Results are written to partitioned user tables in the namespace specified by `OUTPUT_NAMESPACE` (default: `"ExampleReplayTradingSim"`):

- **`TradingSimTrades`**: All executed trades with Date, Timestamp, Symbol, Price, Size, WorkerID
- **`TradingSimPositions`**: Current positions by symbol (cumulative shares held)
- **`TradingSimPnl`**: Profit and loss calculations per symbol
- **`TradingSimPreds`**: Price predictions with EMA-based buy/sell thresholds
- **`TradingSimOrders`**: Current trading signals and position status
- **`TradingSimExecutions`**: Periodic snapshots with action codes
- **`TradingSimSummary`**: Aggregated trade counts and total shares by symbol

All tables include partition columns: `SimulationName`, `WorkerID`, `Date`

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
- 2,500 total sessions (10 workers × 250 days)
- Trades partitioned by date and worker
- Daily PnL by symbol
- Position history
- Strategy performance metrics

All data will be queryable in Deephaven for analysis and visualization.

## Key Features

**Worker Partitioning**: Symbols are distributed across workers using hash-based partitioning:
```python
my_symbols = all_symbols.where(f"(int)(hashCode(Sym)) % {num_workers} == {worker_id}")
```

**Mean-Reversion Strategy**:
- Uses EMA and standard deviation to predict price movements
- Buys when ask < (predicted price - 1 SD)
- Sells when bid > (predicted price + 1 SD)
- Position limits prevent excessive exposure

**Replay Data**: Uses FeedOS `EquityQuoteL1` table filtered for:
- Current replay date via `today()`
- Valid bid/ask quotes (size > 0)
- Symbols assigned to this worker

**Trade Execution**: Evaluates every 10 seconds via `snapshot_when()` and executes based on market conditions and position limits.

## Related Examples

See [`examples/finance/simulated_market_maker`](../../finance/simulated_market_maker) for the original market maker implementation this is based on.

## Customization

To adapt for your use case:

1. **Change stock universe**: Modify the `all_symbols` table in `trading_simulation.py`
2. **Adjust strategy parameters**: Update `MAX_POSITION_DOLLARS`, `EMA_DECAY_TIME`, `LOT_SIZE` in `config.yaml`
3. **Scale workers**: Increase `num_workers` to process more symbols in parallel
4. **Adjust date range**: Modify `dates.start` and `dates.end` in `config.yaml`
5. **Speed up replay**: Increase `replay_speed` for faster backtesting (currently 100x)
