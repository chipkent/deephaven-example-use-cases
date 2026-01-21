# Persistent Query Orchestration Framework

A generic framework for orchestrating Deephaven Enterprise persistent queries across multiple dates with parallel partitions per date. Supports both **replay mode** (live data simulation) and **batch mode** (historical data processing).

## Execution Modes

### Replay Mode

**What are replay persistent queries?** They allow you to run Deephaven queries against historical data as if it were live, making it possible to backtest strategies or reprocess data while maintaining the same code you'd use in production. See [Deephaven Replay Documentation](https://deephaven.io/enterprise/docs/deephaven-database/replayer/) for details.

**Use cases**: Strategy backtesting with realistic data arrival, testing production code on historical periods, reprocessing data with time-aware logic.

### Batch Mode

**What are batch persistent queries?** They process complete historical datasets using vectorized table operations, ideal for analytics and simulations that don't require time-based data arrival simulation.

**Use cases**: Large-scale data analytics, Monte Carlo simulations, parameter sweeps, vectorized strategy backtests.

**Why multiple partitions per date?** Large datasets (e.g., thousands of stocks) can be divided across partitions to process in parallel, dramatically reducing processing time. For example, 10 partitions can each process 1/10th of your stock universe simultaneously.

## Overview

The orchestrator creates and manages persistent queries based on a configuration file. It:

- **Supports dual modes**: Replay (ReplayScript) or Batch (RunAndDone) execution
- **Parallelizes across dates**: Run simulations for multiple dates concurrently
- **Partitions data within each date**: Split data processing across multiple partitions per date
- **Handles retries**: Automatically retries failed sessions
- **Flexible configuration**: Each example has its own complete configuration
- **Generic design**: Works with any worker script

## Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                  Orchestrator                            │
│  - Reads config.yaml                                     │
│  - Authenticates with Deephaven Enterprise               │
│  - Creates (dates × partitions_per_date) PQs             │
│  - Manages concurrency and retries                       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  Persistent Queries (Replay/Batch)   │
        │  - Each session: (date, partition_id) │
        │  - Replay: ReplayScript (live data)  │
        │  - Batch: RunAndDone (historical)    │
        │  - Receives env variables            │
        │  - Executes worker script            │
        │  - Writes to shared tables           │
        └─────────────────────────────────────┘
```

## Use Cases

### Replay Mode

- **Realistic strategy backtesting**: Test trading algorithms with realistic data arrival timing
- **Production code validation**: Verify production queries work correctly on historical periods
- **Time-aware reprocessing**: Reprocess data maintaining temporal relationships

### Batch Mode

- **Vectorized backtesting**: Fast strategy evaluation on complete historical datasets
- **Risk analysis**: Calculate risk metrics across multiple scenarios
- **Data processing**: Process large datasets by partitioning work
- **Monte Carlo simulations**: Run simulations across parameter spaces
- **Parameter sweeps**: Test strategy variations across different parameter combinations

## Directory Structure

```text
pq_orchestration/
├── README.md                                    # This file
├── setup.py                                     # Package setup with dependencies
├── pq_orchestrator.py                          # Generic orchestrator script
├── simple_worker_replay/                        # Minimal replay example
│   ├── simple_worker_replay.py                  # Worker script
│   ├── config.yaml                              # Configuration
│   └── README.md                                # Documentation
├── simple_worker_batch/                         # Minimal batch example
│   ├── simple_worker_batch.py                   # Worker script
│   ├── config.yaml                              # Configuration
│   └── README.md                                # Documentation
├── trading_simulation_replay/                   # Trading replay example
│   ├── trading_simulation_replay.py             # Worker script
│   ├── analyze_trading_results.py               # Analysis tools
│   ├── manage_user_tables.py                    # Table utilities
│   ├── config.yaml                              # Configuration
│   └── README.md                                # Documentation
└── trading_simulation_batch/                    # Trading batch example
    ├── trading_simulation_batch.py              # Worker script
    ├── analyze_trading_results.py               # Analysis tools
    ├── manage_user_tables.py                    # Table utilities
    ├── config.yaml                              # Configuration
    └── README.md                                # Documentation
```

**Core Files**:

- [`setup.py`](setup.py) - Package setup with Python version enforcement
- [`pq_orchestrator.py`](pq_orchestrator.py) - Main orchestrator script (supports both modes)

**Replay Examples**:

- [`simple_worker_replay/`](simple_worker_replay/) - Minimal replay example
- [`trading_simulation_replay/`](trading_simulation_replay/) - Trading simulation with replay

**Batch Examples**:

- [`simple_worker_batch/`](simple_worker_batch/) - Minimal batch example
- [`trading_simulation_batch/`](trading_simulation_batch/) - Trading simulation with vectorized batch processing

## Quick Start

**Prerequisites**: Python 3.10 or higher

### 1. Create Virtual Environment and Install Dependencies

It's recommended to use a virtual environment:

```bash
# Create virtual environment (requires Python 3.10+)
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# Install package and dependencies (enforces Python 3.10+)
pip install -e .
```

### 2. Configure Environment Variables

Set environment variables for Deephaven Enterprise connection and authentication:

```bash
export DH_CONNECTION_URL="https://your-server:8000/iris/connection.json"
export DH_USERNAME="your_username"
export DH_PASSWORD="your_password"
```

### 3. Run a Simple Worker Example

**Choose an example:**

- **Replay mode**: [`simple_worker_replay/config.yaml`](simple_worker_replay/config.yaml) - demonstrates replay with time-based data arrival
- **Batch mode**: [`simple_worker_batch/config.yaml`](simple_worker_batch/config.yaml) - demonstrates batch processing of complete datasets

**Test configuration first (recommended):**

```bash
pq-orchestrator --config simple_worker_replay/config.yaml --dry-run
```

This validates your configuration without creating any sessions.

**Run the orchestrator:**

```bash
pq-orchestrator --config simple_worker_replay/config.yaml
```

This will create 10 sessions (5 weekdays × 2 partitions per date). Monitor progress in the console output. Press Ctrl+C to gracefully stop (finishes current operations before exiting).

## Command-Line Options

```bash
pq-orchestrator --config <path> [--dry-run] [--verbose]
```

**Options:**

- `--config <path>` (required): Path to YAML configuration file
- `--dry-run` (optional): Validate configuration without creating sessions. Useful for testing your setup before running large-scale orchestrations.
- `--verbose` (optional): Enable verbose (DEBUG) logging for detailed troubleshooting. Shows additional information about session capacity, PQ map refreshes, and internal operations.

**Exit Codes:**

- `0`: Success - all sessions created and completed successfully
- `1`: Creation failures - some sessions failed to create after retries
- `2`: Execution failures - some sessions failed during execution
- `3`: Both creation and execution failures occurred
- `4`: Error - orchestrator encountered an exception

**Graceful Shutdown:**

Press `Ctrl+C` (SIGINT) or send SIGTERM to gracefully stop the orchestrator. It will:

- Stop creating new sessions
- Finish processing current operations
- Print a summary of completed and pending work
- Exit with appropriate status code

## Configuration

Each example directory contains a complete `config.yaml` with all orchestration settings. See the [Configuration Reference](#configuration-validation-reference) table for validation rules and limits.

**Before running**: Set the `DH_CONNECTION_URL`, `DH_USERNAME`, and `DH_PASSWORD` environment variables. Optionally adjust config settings for your use case.

### Simulation Name

```yaml
name: "my_simulation"
```

**Parameters:**

- `name` (required): Unique identifier for this simulation run. Used to:
  - Namespace persistent query names:
    - Replay mode: `pq_replay_{name}_{YYYYMMDD}_{partition_id}` (e.g., `pq_replay_mysim_20240115_0`)
    - Batch mode: `pq_batch_{name}_{YYYYMMDD}_{partition_id}` (e.g., `pq_batch_mysim_20240115_0`)
  - Avoid conflicts when running multiple simulations
  - Available to worker scripts via `SIMULATION_NAME` environment variable

### Deephaven Connection

```yaml
deephaven:
  connection_url: "https://your-server:8000/iris/connection.json"
  auth_method: "password"
  username: "${DH_USERNAME}"
  password: "${DH_PASSWORD}"
  private_key_path: ""
```

**Parameters:**

- `connection_url` (required): URL to your Deephaven Enterprise server's connection endpoint (format: `https://host:port/iris/connection.json`)
- `auth_method` (required): Authentication method. Values: `"password"` or `"private_key"`
- `username` (required): Username for authentication. Supports `${VAR}` env var expansion
- `password` (required for password auth): Password for authentication. Supports `${VAR}` env var expansion
- `private_key_path` (required for private_key auth): Path to private key file (absolute or relative to config file directory)

**Note:** Replay sessions are automatically distributed across available query servers by Deephaven Enterprise for optimal load balancing.

### Execution Settings

```yaml
execution:
  worker_script: "worker.py"
  num_partitions: 10
  max_concurrent_sessions: 50
  heap_size_gb: 4.0
  script_language: "Python"
  jvm_profile: "Default"          # Optional
  server_name: "AutoQuery"        # Optional
  max_retries: 3                  # Optional
  max_failures: 10                # Optional
  delete_successful_queries: true # Optional
  delete_failed_queries: false    # Optional
```

**Common Parameters (both modes):**

- `worker_script` (required): Path to worker Python script (absolute or relative to config file directory)
- `num_partitions` (required): Number of parallel partitions per date (range: 1-1000). Each partition receives a unique `PARTITION_ID` (0 to num_partitions-1)
- `max_concurrent_sessions` (required): Maximum total PQ sessions running simultaneously across all dates (range: 1-1000)
- `heap_size_gb` (required): JVM heap size in GB allocated per session (range: >0 to 512)
- `script_language` (required): Worker script language. Values: `"Python"` or `"Groovy"`
- `jvm_profile` (optional, default: `"Default"`): JVM profile defining resource limits and JVM arguments
- `server_name` (optional, default: `"AutoQuery"`): Target query server name. Default uses load balancer for distribution
- `max_retries` (optional, default: 3): Number of retry attempts for failed sessions
- `max_failures` (optional, default: 10): Maximum execution failures before aborting
- `delete_successful_queries` (optional, default: true): Auto-delete successful queries after completion
- `delete_failed_queries` (optional, default: false): Auto-delete failed queries (default: preserve for debugging)

### Replay Settings (Replay Mode Only)

**Note**: Use exactly **one** of `replay:` or `batch:` sections to define execution mode.

```yaml
replay:
  init_timeout_minutes: 10
  replay_start: "09:30:00"
  replay_speed: 100.0
  sorted_replay: true          # Optional
  buffer_rows: 10000           # Optional
  replay_timestamp_columns:    # Optional
    - namespace: "Market"
      table: "Trade"
      column: "EventTime"
```

**Required Parameters:**

- `init_timeout_minutes` (required): How long to wait for PQ to initialize/start up, in minutes
- `replay_start` (required): Time when replay starts each day, format `HH:MM:SS`
- `replay_speed` (required): Speed multiplier for replay (range: 1.0-100.0)

**Replay Behavior:**

- `replay_start` (required): Time when replay starts each day, format `HH:MM:SS`
- `replay_speed` (required): Speed multiplier for replay (range: 1.0-100.0)
  - `1.0` = real-time (1 historical second = 1 replay second)
  - `10.0` = 10x speed for faster backtesting
  - `100.0` = maximum speed (limited to ensure update cycles stay >= 10ms)
- `sorted_replay` (optional, default: true): Guarantee timestamp-ordered data delivery

**Automatic Update Frequency Scaling:**

When `replay_speed` > 1.0, the orchestrator **automatically** sets `PeriodicUpdateGraph.targetCycleDurationMillis` to maintain the simulated update frequency.

**How it works**: In real-time replay (1x speed), update cycles run at ~1 second, processing 1 second of historical data per cycle. At higher speeds, the orchestrator scales the update cycle duration to maintain the same simulated update frequency. The formula is:

```python
targetCycleDurationMillis = 1000 / replay_speed
```

**Examples**:

- `replay_speed: 10` → 100ms cycle duration (maintains 10 Hz simulated update frequency)
- `replay_speed: 60` → 16ms cycle duration (maintains 60 Hz simulated update frequency)
- `replay_speed: 100` → 10ms cycle duration (maintains 100 Hz simulated update frequency)

**Minimum**: Target cycle duration cannot go below 10ms, which limits `replay_speed` to a maximum of 100.

**Note**: If your script is complex and cannot complete within the target cycle duration, Deephaven will add more data to subsequent cycles to catch up, potentially causing performance issues. Ensure your queries can execute within the scaled cycle time.

**Optional Replay Settings:**

- `sorted_replay` (optional, default: true): Guarantee timestamp-ordered data delivery
- `buffer_rows` (optional): Number of rows to buffer during replay (sets `-DReplayDatabase.BufferSize`)
- `replay_timestamp_columns` (optional): Per-table timestamp column overrides

**Replay Database Settings:**

- `buffer_rows` (optional): Number of rows to buffer during replay (sets `-DReplayDatabase.BufferSize`). See [Replay Database Settings](https://deephaven.io/enterprise/docs/deephaven-database/replayer/) for details.
- `replay_timestamp_columns` (optional): List of per-table timestamp column configurations. **Only required if your tables use a timestamp column name other than `Timestamp`**. By default, replay automatically uses the `Timestamp` column, or if a table has only one `Instant` column, it uses that. Specify this parameter to override the default for specific tables:

  ```yaml
  replay_timestamp_columns:
    - namespace: "Market"
      table: "Trade"
      column: "EventTime"
    - namespace: "Market"
      table: "Quote"
      column: "QuoteTimestamp"
  ```

  Each entry sets `-DReplayDatabase.TimestampColumn.{namespace}.{table}={column}`

**Note:** The framework automatically uses `ReplayScript` configuration type and fixed replay time type, which are optimal for backtesting scenarios. Persistent queries run immediately upon creation with a continuous scheduler.

### Batch Settings (Batch Mode Only)

**Note**: Use exactly **one** of `replay:` or `batch:` sections to define execution mode.

```yaml
batch:
  timeout_minutes: 60
```

**Parameters:**

- `timeout_minutes` (required): Maximum runtime for each batch query in minutes. Batch queries (RunAndDone type) require a mandatory timeout. If a query exceeds this timeout, it will be terminated.

**Batch Behavior:**

- Queries use `RunAndDone` configuration type (auto-terminate after completion)
- Scheduling is disabled (queries start immediately upon creation)
- Worker scripts should use `db.historical_table()` instead of `db.live_table()`
- No automatic `stop_and_wait()` call needed (RunAndDone handles termination)
- Ideal for vectorized processing of complete historical datasets

**Example batch worker:**

```python
# Get complete historical data for the date
ticks = db.historical_table("FeedOS", "EquityQuoteL1") \
    .where(f"Date == '{simulation_date}'")

# Process using vectorized operations
results = ticks.group_by(["Sym"]) \
    .update(["Metric = compute_metric_udf(Price, Volume)"]) \
    .ungroup()
```

### Date Range

```yaml
dates:
  start: "2024-01-01"
  end: "2024-12-31"
  weekdays_only: true
```

**Parameters:**

- `start` (required): First date to process, format `YYYY-MM-DD`
- `end` (required): Last date to process (inclusive), format `YYYY-MM-DD`
- `weekdays_only` (optional, default: false): If true, skip Saturdays and Sundays

### Environment Variables

```yaml
env:
  OUTPUT_TABLE: "results"
  CUSTOM_PARAM: "value"
```

**Requirements:**

The `env` section must be a dictionary (can be empty: `env: {}`, but cannot be `null` or a non-dict type).

**Custom Variables:**

All variables defined in the `env` section are passed to every worker session. Use these for configuration parameters your worker script needs.

**Auto-Generated Variables:**

The orchestrator automatically sets these environment variables for each worker session:

- `SIMULATION_NAME`: The simulation name from the config
- `SIMULATION_DATE`: The date being processed (YYYY-MM-DD string format)
- `PARTITION_ID`: Partition ID (0 to NUM_PARTITIONS-1) for dividing work across partitions
- `NUM_PARTITIONS`: Total number of partitions per date
- `QUERY_NAME`: The persistent query name (format: `pq_{mode}_{name}_{YYYYMMDD}_{partition_id}` where mode is "replay" or "batch")

**Note:** For date operations in your worker script, use [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today) from the [`deephaven.time`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html) module rather than `SIMULATION_DATE`. The [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today) function works correctly in both replay and production environments.

## Creating a Worker Script

Worker scripts access environment variables to determine their assigned work partition.

```python
import os
from deephaven.time import dh_today

# Get date using dh_today() - works in both backtesting and production
date = dh_today()  # "2024-01-15" during replay

# Read data partitioning variables
partition_id = int(os.getenv("PARTITION_ID"))      # 0-9 for this date
num_partitions = int(os.getenv("NUM_PARTITIONS"))  # 10 partitions per date

# Alternative: SIMULATION_DATE environment variable (for debugging)
date_str = os.getenv("SIMULATION_DATE")  # "2024-01-15"

# Partition work across partitions (example: split stocks across partitions for this date)
all_stocks = get_stock_list()
my_stocks = all_stocks[partition_id::num_partitions]  # This partition processes every Nth stock

# Process data for this date and stocks
process_data(date, my_stocks)

# Write to shared partitioned tables
# (partitioning is handled by Deephaven)
```

## Examples

### Simple Worker Examples

**Replay Mode**: [`simple_worker_replay/`](simple_worker_replay/)

- Minimal example demonstrating replay mode with time-based data arrival simulation
- Creates a status table to verify orchestration is working
- **Scale**: 2 partitions per date × 5 weekdays = 10 sessions
- See [`simple_worker_replay/README.md`](simple_worker_replay/README.md)

**Batch Mode**: [`simple_worker_batch/`](simple_worker_batch/)

- Minimal example demonstrating batch mode for processing complete datasets
- Creates a status table to verify orchestration is working
- **Scale**: 2 partitions per date × 5 weekdays = 10 sessions
- See [`simple_worker_batch/README.md`](simple_worker_batch/README.md)

### Trading Simulation Examples

**Replay Mode**: [`trading_simulation_replay/`](trading_simulation_replay/)

- Mean-reversion trading strategy with realistic time-based data arrival
- Processes historical market data as if it were live
- **Scale**: 2 partitions per date × 250 trading days = 500 sessions
- Based on [`examples/finance/simulated_market_maker`](../finance/simulated_market_maker)
- See [`trading_simulation_replay/README.md`](trading_simulation_replay/README.md)

**Batch Mode**: [`trading_simulation_batch/`](trading_simulation_batch/)

- Vectorized trading strategy using batch processing
- Processes complete historical datasets with optimized table operations
- **Scale**: 2 partitions per date × 250 trading days = 500 sessions
- Uses numba-optimized UDF for high-performance computation
- See [`trading_simulation_batch/README.md`](trading_simulation_batch/README.md)

## Monitoring and Progress

### Console Output

The orchestrator provides real-time progress information:

```text
[2024-01-15 10:30:00] INFO: Created session 5/60: date=2024-01-15, partition=2, serial=12345
[2024-01-15 10:30:05] INFO: Session completed successfully: date=2024-01-15, partition=0
[2024-01-15 10:30:10] WARNING: No progress for 10 iterations. Created: 10, Active: 5, Pending: 45, Completed: 5, Failed: 0
```

**Progress Tracking:**

- Session creation with serial numbers
- Completion notifications
- Failure notifications with status codes
- Stall detection warnings (no progress for 10 iterations)
- Final summary with success/failure counts

### Viewing Created Persistent Queries

In the Deephaven Enterprise UI:

1. Navigate to **Persistent Queries** section
2. Look for queries named: `pq_{mode}_{simulation_name}_{date}_{partition_id}` (where mode is "replay" or "batch")
3. Filter by `pq_` prefix to see only orchestrated queries
4. Check status: Running, Completed, Failed, etc.
5. View logs and output tables for each query

### Output Tables

Worker scripts write to shared tables. Access them:

- In Deephaven UI: Navigate to the table namespace
- Via query: Tables created by worker scripts are accessible in the Deephaven session
- Results are partitioned automatically by Deephaven

## How It Works

1. **Configuration**: Load worker's `config.yaml` and validate all settings
2. **Authentication**: Connect to Deephaven Enterprise with configured credentials
3. **Task Generation**: Create (dates × partitions_per_date) combinations
4. **Session Creation**: For each task:
   - Build `PersistentQueryConfigMessage` with replay parameters
   - Set replay date, speed, time in `typeSpecificFieldsJson`
   - Add environment variables (SIMULATION_NAME, SIMULATION_DATE, PARTITION_ID, NUM_PARTITIONS, custom vars)
   - Create replay persistent query via controller client
5. **Concurrency Management**: Run up to `max_concurrent_sessions` using subscription-based status monitoring
6. **Retry Logic**: Retry failed session creation up to `max_retries` times with exponential backoff
7. **Status Monitoring**: Subscribe to persistent query status changes, distinguish completed vs failed terminal states
8. **Completion**: Report success/failure summary with detailed counts and failed session list

## Replay Parameters

The orchestrator configures replay persistent queries with:

- **Replay Date**: Set in `typeSpecificFieldsJson.replayDate`
- **Replay Speed**: Set in `typeSpecificFieldsJson.replaySpeed`
- **Replay Start Time**: Set in `typeSpecificFieldsJson.replayStart` (simulated time, not execution time)
- **Sorted Replay**: Set in `typeSpecificFieldsJson.sortedReplay`
- **JVM Arguments**: Advanced replay settings via `-DReplayDatabase.*`
- **Scheduler**: Disabled scheduler (immediate execution without time constraints)

## Best Practices

1. **Start Small**: Test with a small date range and few partitions first to verify your setup is working correctly.

2. **Test Your Worker Script**: Verify your worker script functions correctly before running large-scale orchestration.

3. **Monitor Resources**: Check your Deephaven server capacity and adjust `max_concurrent_sessions` accordingly.

4. **Partition Data Efficiently**: Design your data partitioning logic to distribute work evenly across partitions.

5. **Use Weekdays Only**: For financial data, enable `weekdays_only: true` to skip weekends.

6. **Adjust Replay Speed**: Use `replay_speed` parameter to control how fast historical data is replayed.

## Troubleshooting

### Configuration Validation Errors

Run with `--dry-run` first to catch configuration errors:

```bash
python pq_orchestrator.py --config your_config.yaml --dry-run  # Run directly
pq-orchestrator --config your_config.yaml --dry-run            # Or use installed command
```

Common validation errors:

- Missing required fields (name, heap_size_gb, worker_script, etc.)
- Invalid value ranges (heap_size_gb > 512, num_partitions > 1000, replay_speed > 100)
- Wrong types (env must be a dictionary, not null)
- Invalid date format (must be YYYY-MM-DD)

### Authentication Errors

Ensure environment variables are set:

```bash
echo $DH_CONNECTION_URL
echo $DH_USERNAME
echo $DH_PASSWORD
```

If using private key authentication:

```bash
# Verify key file exists
ls -la /path/to/private_key
```

### Worker Script Not Found

Worker script path is **relative to the config file directory**, not the current working directory.

Example: If config is at [`simple_worker_replay/config.yaml`](simple_worker_replay/config.yaml) and specifies `worker_script: "simple_worker_replay.py"`, the script must be at [`simple_worker_replay/simple_worker_replay.py`](simple_worker_replay/simple_worker_replay.py).

### Session Creation Failures

Check Deephaven server logs for detailed error messages. Common issues:

- **Insufficient heap size**: Increase `heap_size_gb` in config
- **Invalid replay parameters**: Verify replay_start format (HH:MM:SS), replay_speed range (1.0-100.0)
- **Script syntax errors**: Test worker script independently before orchestration
- **Missing environment variables**: Ensure DH_CONNECTION_URL, DH_USERNAME, DH_PASSWORD are set
- **Connection refused**: Verify connection_url is correct and server is accessible

### Replay Date Issues

Ensure replay dates have available historical data in your Deephaven database. Check:

- Data exists for the date range specified in `dates.start` to `dates.end`
- Replay tables are properly configured in Deephaven
- Timestamp columns match your data

### Progress Stalls

If you see "No progress for 10 iterations" warnings (30 iterations during startup phase):

- **During startup**: This is normal as queries initialize and acquire workers (threshold: 30 iterations)
- **After startup**: Check Deephaven server capacity (may be at max concurrent sessions)
- Reduce `max_concurrent_sessions` if server is overloaded
- Check for failed sessions in console output
- Verify sessions aren't stuck in initialization (check `init_timeout_minutes`)

### Graceful Shutdown Not Working

If Ctrl+C doesn't stop gracefully:

- First Ctrl+C initiates graceful shutdown
- Second Ctrl+C forces immediate termination
- Check console for "Shutdown signal received" message

## Configuration Validation Reference

The orchestrator validates all configuration before execution. Here's a quick reference:

| Parameter | Type | Range/Values | Default | Required |
| --------- | ---- | ------------ | ------- | -------- |
| `name` | string | non-empty | - | Yes |
| `connection_url` | string | non-empty | - | Yes |
| `auth_method` | string | "password" or "private_key" | - | Yes |
| `username` | string | non-empty | - | Yes |
| `password` | string | non-empty | - | Conditional (password auth) |
| `private_key_path` | string | non-empty | - | Conditional (private_key auth) |
| `worker_script` | string | non-empty | - | Yes |
| `heap_size_gb` | number | >0 to 512 | - | Yes |
| `replay_start` | string | HH:MM:SS | - | Yes |
| `replay_speed` | number | 1.0-100.0 | - | Yes |
| `script_language` | string | "Python" or "Groovy" | - | Yes |
| `num_partitions` | int | 1-1000 | - | Yes |
| `max_concurrent_sessions` | int | 1-1000 | - | Yes |
| `max_retries` | int | ≥0 | 3 | No |
| `delete_successful_queries` | bool | true/false | true | No |
| `delete_failed_queries` | bool | true/false | false | No |
| `init_timeout_minutes` | number | >0 | 1 | No |
| `buffer_rows` | int | >0 | - | No |
| `sorted_replay` | bool | true/false | true | No |
| `jvm_profile` | string | non-empty | "Default" | No |
| `server_name` | string | non-empty | "AutoQuery" | No |
| `dates.start` | string | YYYY-MM-DD | - | Yes |
| `dates.end` | string | YYYY-MM-DD | - | Yes |
| `weekdays_only` | bool | true/false | false | No |
| `env` | dict | must be dict | - | Yes (can be empty `{}`) |

## Requirements

- [Deephaven Enterprise](https://deephaven.io/enterprise/) with [replay functionality](https://deephaven.io/enterprise/docs/deephaven-database/replayer/)
- Python 3.10 or higher
- [Deephaven Enterprise Python client](https://deephaven.io/enterprise/docs/python-client/) (`deephaven-enterprise` package)
- Historical data in Deephaven database configured for replay
- Access to Deephaven Enterprise server with persistent query capabilities

## Additional Resources

- [Deephaven Enterprise Documentation](https://deephaven.io/enterprise/docs/)
- [Replay Database Documentation](https://deephaven.io/enterprise/docs/deephaven-database/replayer/)
- [Persistent Query API](https://deephaven.io/enterprise/docs/python-client/persistent-query-api/)
- [Deephaven Time Functions](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html)
- [Example: Simulated Market Maker](../finance/simulated_market_maker/)
