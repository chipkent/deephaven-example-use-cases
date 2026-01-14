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
my_symbols = all_symbols.where(f"(int)(hashCode(Sym)) % {num_workers} == {worker_id}")
```

**Note**: This example uses 10 hardcoded symbols (AAPL, GOOG, MSFT, etc.) for demonstration. To scale to larger universes like SP500, modify the `all_symbols` table in `trading_simulation.py`.

### Configuration

This example is configured for large-scale backtesting:

```yaml
execution:
  num_workers: 10              # 10 workers per date (each processes a subset of stocks)
  max_concurrent_sessions: 50  # Max total sessions running concurrently
  
replay:
  heap_size_gb: 8.0            # More RAM for trading simulation
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
- `WORKER_ID`: Which stock partition this worker handles (0 to NUM_WORKERS-1)
- `NUM_WORKERS`: Total workers per date (for partitioning logic)

## Output Tables

Each worker creates:

- **`trades`**: All executed trades with Date, Timestamp, Symbol, Price, Size, WorkerID
- **`positions`**: Current positions by symbol (cumulative shares held)
- **`pnl`**: Profit and loss calculations per symbol
- **`preds`**: Price predictions with EMA-based buy/sell thresholds
- **`orders`**: Current trading signals and position status
- **`executions`**: Snapshot of trade decisions every 10 seconds
- **`trade_summary`**: Aggregated trade counts and total shares by symbol

## Running

```bash
replay-orchestrator --config trading_simulation/config.yaml
```

**Note**: This requires Deephaven Enterprise with FeedOS access for historical equity quote data.

## Expected Output

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
