# Replay Orchestration Framework

A generic framework for orchestrating Deephaven Enterprise replay persistent queries across multiple dates with parallel workers per date.

**What are replay persistent queries?** They allow you to run Deephaven queries against historical data as if it were live, making it possible to backtest strategies or reprocess data while maintaining the same code you'd use in production. See [Deephaven Replay Documentation](https://deephaven.io/enterprise/docs/deephaven-database/replayer/) for details.

**Why multiple workers per date?** Large datasets (e.g., thousands of stocks) can be partitioned across workers to process in parallel, dramatically reducing backtest time. For example, 10 workers can each process 1/10th of your stock universe simultaneously.

## Overview

The replay orchestrator creates and manages replay persistent queries based on a configuration file. It:

- **Parallelizes across dates**: Run simulations for multiple dates concurrently
- **Partitions data within each date**: Split data processing across multiple workers per date
- **Handles retries**: Automatically retries failed sessions
- **Flexible configuration**: Each example has its own complete configuration
- **Generic design**: Works with any worker script

## Architecture

```text
┌─────────────────────────────────────────────────────────┐
│                  Orchestrator                            │
│  - Reads config.yaml                                     │
│  - Authenticates with Deephaven Enterprise               │
│  - Creates (dates × workers_per_date) replay queries     │
│  - Manages concurrency and retries                       │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
        ┌─────────────────────────────────────┐
        │  Replay Persistent Queries           │
        │  - Each session: (date, worker_id)   │
        │  - Receives env variables            │
        │  - Executes worker script            │
        │  - Writes to shared tables           │
        └─────────────────────────────────────┘
```

## Use Cases

- **Backtesting trading strategies**: Simulate trading across historical periods
- **Risk analysis**: Calculate risk metrics across multiple scenarios
- **Data processing**: Process large datasets by partitioning work
- **Monte Carlo simulations**: Run simulations across parameter spaces

## Directory Structure

```text
replay_orchestration/
├── README.md                    # This file
├── setup.py                     # Package setup with dependencies
├── replay_orchestrator.py      # Generic orchestrator script
├── simple_worker/               # Minimal example
│   ├── simple_worker.py
│   ├── config.yaml
│   └── README.md
└── trading_simulation/          # Trading example (placeholder)
    ├── trading_simulation.py
    ├── config.yaml
    └── README.md
```

**Files**:

- [`setup.py`](setup.py) - Package setup with Python version enforcement
- [`replay_orchestrator.py`](replay_orchestrator.py) - Main orchestrator script
- [`simple_worker/simple_worker.py`](simple_worker/simple_worker.py) - Simple example worker
- [`simple_worker/config.yaml`](simple_worker/config.yaml) - Simple worker configuration
- [`simple_worker/README.md`](simple_worker/README.md) - Simple worker documentation
- [`trading_simulation/trading_simulation.py`](trading_simulation/trading_simulation.py) - Trading simulation worker (placeholder implementation)
- [`trading_simulation/config.yaml`](trading_simulation/config.yaml) - Trading configuration
- [`trading_simulation/README.md`](trading_simulation/README.md) - Trading documentation

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

### 3. Run the Simple Worker Example

**Test configuration first (recommended):**

```bash
replay-orchestrator --config simple_worker/config.yaml --dry-run
```

This validates your configuration without creating any sessions.

**Run the orchestrator:**

```bash
replay-orchestrator --config simple_worker/config.yaml
```

This will create 6 replay sessions (3 dates × 2 workers per date). Monitor progress in the console output. Press Ctrl+C to gracefully stop (finishes current operations before exiting).

## Command-Line Options

```bash
replay-orchestrator --config <path> [--dry-run] [--verbose]
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
  - Namespace persistent query names (format: `replay_{name}_{date}_{worker_id}`)
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
- `private_key_path` (required for private_key auth): Path to private key file (absolute or relative to config file)

**Note:** Replay sessions are automatically distributed across available query servers by Deephaven Enterprise for optimal load balancing.

### Execution Settings

```yaml
execution:
  worker_script: "simple_worker.py"
  num_workers: 10
  max_concurrent_sessions: 50
  max_retries: 3
```

**Parameters:**

- `worker_script` (required): Path to worker Python script, relative to config file directory
- `num_workers` (required): Number of parallel workers per date (range: 1-1000). Each worker receives a unique `WORKER_ID` (0 to num_workers-1)
- `max_concurrent_sessions` (optional, default: 50, max: 1000): Maximum total replay sessions running simultaneously across all dates
- `max_retries` (optional, default: 3): Number of retry attempts for failed sessions

### Replay Settings

```yaml
replay:
  heap_size_gb: 4.0
  init_timeout_minutes: 10
  script_language: "Python"
  jvm_profile: "Default"
  replay_start: "09:30:00"
  replay_speed: 1.0
  sorted_replay: true
  buffer_rows: 10000
```

**Core Parameters:**

- `heap_size_gb` (required): JVM heap size in GB allocated per session (range: >0 to 512, e.g., 4.0, 8.0)
- `init_timeout_minutes` (optional, default: 1): How long to wait for PQ to initialize/start up, in minutes
- `script_language` (required): Worker script language. Values: `"Python"` or `"Groovy"`
- `jvm_profile` (optional, default: `"Default"`): JVM profile defining resource limits and JVM arguments
- `server_name` (optional, default: `"AutoQuery"`): Target query server name for replay sessions. The default `"AutoQuery"` uses the Deephaven load balancer to automatically distribute queries across available servers for optimal load balancing. You can specify a specific server name (e.g., `"Query_1"`, `"Query_2"`) to target a particular server if needed.

**Replay Behavior:**

- `replay_time` (optional, default: `"09:30:00"`): Time when replay starts each day, format `HH:MM:SS`
- `replay_speed` (optional, default: 1.0): Speed multiplier for replay (range: 1.0-100.0)
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

**Replay Database Settings:**

- `buffer_rows` (optional, default: 10000): Number of rows to buffer during replay (sets `-DReplayDatabase.BufferSize`). See [Replay Database Settings](https://deephaven.io/enterprise/docs/deephaven-database/replayer/) for details.
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

**Note:** The framework automatically uses `ReplayScript` configuration type and fixed replay time type, which are optimal for backtesting scenarios.

### Scheduler

```yaml
scheduler:
  calendar: "USNYSE"
  start_time: "09:30:00"
  stop_time: "16:00:00"
  timezone: "America/New_York"
  business_days: true
```

The `scheduler` section is **optional**. If present, it configures the Deephaven internal scheduler for the Persistent Query, which controls when the PQ automatically starts and stops each day. These parameters are passed to [`GenerateScheduling.generate_daily_scheduler()`](https://deephaven.io/enterprise/docs/python-client/persistent-query-api/). If the section is omitted, the Persistent Query runs immediately upon creation without time-of-day constraints.

**Parameters (all required if section is present):**

- `calendar` (required): Business calendar for determining valid trading days (e.g., `"USNYSE"`)
- `start_time` (required): Time when PQ starts each day, format `HH:MM:SS`
- `stop_time` (required): Time when PQ stops each day, format `HH:MM:SS`
- `timezone` (required): Timezone for scheduler times (e.g., `"America/New_York"`)
- `business_days` (required): If `true`, only run on business days according to the calendar

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
The orchestrator automatically sets these environment variables for each session:

- `SIMULATION_NAME`: The simulation name from config (for namespacing outputs and logging)
- `SIMULATION_DATE`: The date being processed, format `YYYY-MM-DD` (also available via [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today))
- `WORKER_ID`: Worker identifier, range `0` to `NUM_WORKERS-1`, for partitioning data across workers
- `NUM_WORKERS`: Total number of workers per date

**Note on SIMULATION_DATE**: While the replay date is also accessible via [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today) from the [`deephaven.time`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html) module, the `SIMULATION_DATE` environment variable provides an explicit string value for debugging purposes. [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today) is preferred for all date operations (including logging and table queries) because it works in both backtesting and non-backtesting scenarios.

## Creating a Worker Script

Worker scripts receive environment variables and process data for their assigned date and worker ID.

For date-based operations, you can use either the `SIMULATION_DATE` environment variable or [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today) from the [`deephaven.time`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html) module.

```python
import os
from deephaven.time import dh_today

# Get date using dh_today() - works in both backtesting and production
date = dh_today()  # "2024-01-15" during replay

# Read worker partitioning variables
worker_id = int(os.getenv("WORKER_ID"))      # 0-9 for this date
num_workers = int(os.getenv("NUM_WORKERS"))  # 10 workers per date

# Alternative: SIMULATION_DATE environment variable (for debugging)
date_str = os.getenv("SIMULATION_DATE")  # "2024-01-15"

# Partition work across workers (example: split stocks across workers for this date)
all_stocks = get_stock_list()
my_stocks = all_stocks[worker_id::num_workers]  # This worker processes every Nth stock

# Process data for this date and stocks
process_data(date, my_stocks)

# Write to shared partitioned tables
# (partitioning is handled by Deephaven)
```

## Examples

### Simple Worker

Location: [`simple_worker/`](simple_worker/)

**Purpose**: Minimal example to verify your orchestration setup works correctly.

**What it does**: Creates a status table showing which worker processed which date.

**Scale**: 2 workers per date × 3 weekdays (Jan 1-5, 2024) = 6 sessions

See [`simple_worker/README.md`](simple_worker/README.md) for details.

### Trading Simulation

Location: [`trading_simulation/`](trading_simulation/)

**Status**: Placeholder implementation - demonstrates configuration but trading logic not yet implemented.

**Purpose**: Backtest a mean-reversion market making strategy across a full year.

**Planned features**:

- Simulate trading for different stock subsets in parallel
- Write trades/positions/PnL to partitioned tables
- Based on [`examples/finance/simulated_market_maker`](../finance/simulated_market_maker)

**Scale**: 10 workers per date × 250 trading days = 2,500 sessions

See [`trading_simulation/README.md`](trading_simulation/README.md) for implementation details.

## Monitoring and Progress

### Console Output

The orchestrator provides real-time progress information:

```text
[2024-01-15 10:30:00] INFO: Created session 5/60: date=2024-01-15, worker=2, serial=12345
[2024-01-15 10:30:05] INFO: Session completed successfully: date=2024-01-15, worker=0
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
2. Look for queries named: `replay_{simulation_name}_{date}_{worker_id}`
3. Check status: Running, Completed, Failed, etc.
4. View logs and output tables for each query

### Output Tables

Worker scripts write to shared tables. Access them:

- In Deephaven UI: Navigate to the table namespace
- Via query: Tables created by worker scripts are accessible in the Deephaven session
- Results are partitioned automatically by Deephaven

## How It Works

1. **Configuration**: Load worker's `config.yaml` and validate all settings
2. **Authentication**: Connect to Deephaven Enterprise with configured credentials
3. **Task Generation**: Create (dates × workers_per_date) combinations
4. **Session Creation**: For each task:
   - Build `PersistentQueryConfigMessage` with replay parameters
   - Set replay date, speed, time in `typeSpecificFieldsJson`
   - Add environment variables (SIMULATION_NAME, SIMULATION_DATE, WORKER_ID, NUM_WORKERS, custom vars)
   - Create replay persistent query via controller client
5. **Concurrency Management**: Run up to `max_concurrent_sessions` using subscription-based status monitoring
6. **Retry Logic**: Retry failed session creation up to `max_retries` times with exponential backoff
7. **Status Monitoring**: Subscribe to persistent query status changes, distinguish completed vs failed terminal states
8. **Completion**: Report success/failure summary with detailed counts and failed session list

## Replay Parameters

The orchestrator configures replay persistent queries with:

- **Replay Date**: Set in `typeSpecificFieldsJson.replayDate`
- **Replay Speed**: Set in `typeSpecificFieldsJson.replaySpeed`
- **Replay Time**: Set in `typeSpecificFieldsJson.replayTime`
- **Sorted Replay**: Set in `typeSpecificFieldsJson.sortedReplay`
- **JVM Arguments**: Advanced replay settings via `-DReplayDatabase.*`
- **Scheduler**: Calendar, time windows, timezone (Deephaven internal PQ scheduler)

## Best Practices

1. **Start Small**: Test with a small date range and few workers first to verify your setup is working correctly.

2. **Test Your Worker Script**: Verify your worker script functions correctly before running large-scale orchestration.

3. **Monitor Resources**: Check your Deephaven server capacity and adjust `max_concurrent_sessions` accordingly.

4. **Partition Efficiently**: Design your worker partitioning logic to distribute work evenly across workers.

5. **Use Weekdays Only**: For financial data, enable `weekdays_only: true` to skip weekends.

6. **Adjust Replay Speed**: Use `replay_speed` parameter to control how fast historical data is replayed.

## Troubleshooting

### Configuration Validation Errors

Run with `--dry-run` first to catch configuration errors:

```bash
python replay_orchestrator.py --config your_config.yaml --dry-run
```

Common validation errors:

- Missing required fields (name, heap_size_gb, worker_script, etc.)
- Invalid value ranges (heap_size_gb > 512, num_workers > 1000, replay_speed > 1000)
- Wrong types (env must be a dictionary, not null)
- Invalid date format (must be YYYY-MM-DD)
- Scheduler section incomplete (all 5 fields required if present)

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

Example: If config is at `simple_worker/config.yaml` and specifies `worker_script: "simple_worker.py"`, the script must be at `simple_worker/simple_worker.py`.

### Session Creation Failures

Check Deephaven server logs for detailed error messages. Common issues:

- **Insufficient heap size**: Increase `heap_size_gb` in config
- **Invalid replay parameters**: Verify replay_time format (HH:MM:SS), replay_speed range (1.0-1000.0)
- **Script syntax errors**: Test worker script independently before orchestration
- **Missing environment variables**: Ensure DH_CONNECTION_URL, DH_USERNAME, DH_PASSWORD are set
- **Connection refused**: Verify connection_url is correct and server is accessible

### Replay Date Issues

Ensure replay dates have available historical data in your Deephaven database. Check:

- Data exists for the date range specified in `dates.start` to `dates.end`
- Replay tables are properly configured in Deephaven
- Timestamp columns match your data

### Progress Stalls

If you see "No progress for 10 iterations" warnings:

- Check Deephaven server capacity (may be at max concurrent sessions)
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
| `username` | string | non-empty | - | Yes |
| `worker_script` | string | non-empty | - | Yes |
| `heap_size_gb` | number | >0 to 512 | - | Yes |
| `replay_start` | string | HH:MM:SS | - | Yes |
| `replay_speed` | number | 1.0-100.0 | - | Yes |
| `script_language` | string | "Python" or "Groovy" | - | Yes |
| `num_workers` | int | 1-1000 | - | Yes |
| `max_concurrent_sessions` | int | 1-1000 | 50 | No |
| `max_retries` | int | ≥0 | 3 | No |
| `init_timeout_minutes` | number | >0 | 1 | No |
| `buffer_rows` | int | >0 | 10000 | No |
| `sorted_replay` | bool | true/false | true | No |
| `server_name` | string | non-empty | "AutoQuery" | No |
| `dates.start` | string | YYYY-MM-DD | - | Yes |
| `dates.end` | string | YYYY-MM-DD | - | Yes |
| `weekdays_only` | bool | true/false | false | No |
| `env` | dict | must be dict | - | Yes (can be empty `{}`) |
| `scheduler` | dict | all 5 fields if present | - | No (omit entirely or include all fields) |
| `scheduler.start_time` | string | HH:MM:SS | - | Yes (if scheduler present) |
| `scheduler.stop_time` | string | HH:MM:SS | - | Yes (if scheduler present) |
| `scheduler.business_days` | bool | true/false | - | Yes (if scheduler present) |

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
