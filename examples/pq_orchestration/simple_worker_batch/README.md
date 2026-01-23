# Simple Worker Batch Example

A minimal batch mode example to **verify the PQ orchestration framework is working correctly**.

## Purpose

This example is designed to:

- **Test the orchestrator**: Verify batch mode sessions are created and executed successfully
- **Validate environment variables**: Confirm all expected variables are passed correctly
- **Demonstrate batch patterns**: Show how batch queries auto-terminate (no `stop_and_wait()` needed)
- **Print diagnostic output**: Display values to console for verification

**Note**: This example does **not** persist any data. It creates an in-memory table and prints values to demonstrate the orchestrator is working.

**Replay alternative**: For a replay version that simulates time-based data arrival, see [`simple_worker_replay/`](../simple_worker_replay/).

## Key Differences from Replay Version

- Uses `batch:` configuration section instead of `replay:`
- Configured with `timeout_minutes` instead of `init_timeout_minutes`
- No `stop_and_wait()` call (RunAndDone auto-terminates)
- Persistent queries named `pq_batch_*` instead of `pq_replay_*`

## Environment Variables

The orchestrator automatically provides:

- `SIMULATION_NAME`: Unique simulation identifier from config top-level `name` field
- `SIMULATION_DATE`: The date being processed (YYYY-MM-DD) - also available via [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today)
- `PARTITION_ID`: The partition ID (0 to NUM_PARTITIONS-1) for partitioning data
- `NUM_PARTITIONS`: Total number of partitions per date

From [`config.yaml`](config.yaml) env section:

- `LOG_LEVEL`: Logging level
- `CUSTOM_MESSAGE`: Example custom parameter (demonstrates how to pass configuration to workers)

## Configuration

See [`config.yaml`](config.yaml) for the complete orchestrator configuration. Key settings:

```yaml
execution:
  worker_script: "simple_worker_batch.py"
  num_partitions: 2
  max_concurrent_sessions: 10
  heap_size_gb: 4.0
  script_language: "Python"

batch:
  timeout_minutes: 5

dates:
  start: "2024-01-01"
  end: "2024-01-05"
  weekdays_only: true         # Only Mon-Fri (5 weekdays in this range)
```

This creates 2 partitions per date × 5 weekdays = 10 total batch sessions.

## Running

From the `pq_orchestration` directory:

```bash
pq-orchestrator --config simple_worker_batch/config.yaml
```

## Output

Each session prints to console:

- `SIMULATION_NAME`: The simulation identifier
- `SIMULATION_DATE`: The date being processed
- `dh_today()`: Comparison to verify date matches
- `PARTITION_ID`: This partition's ID
- `NUM_PARTITIONS`: Total number of partitions per date
- `CUSTOM_MESSAGE`: The custom message from config
- Status messages confirming execution

The script also creates an in-memory table (`worker_status`) with the following columns:

- `Date`: The simulation date
- `PartitionID`: This partition's ID
- `NumPartitions`: Total partitions per date
- `Status`: Always "COMPLETED" in this example
- `Message`: Descriptive message showing partition and date
- `CustomMessage`: The custom message from config

This table is **not published or persisted** - it's purely for demonstration purposes.

## Verification

After running, check the orchestrator console output to verify:

- All 10 sessions (2 partitions × 5 weekdays) were created
- Each session completed successfully
- No failures reported
- Exit code 0 (success)

## Files

- [`simple_worker_batch.py`](simple_worker_batch.py) - Worker script
- [`config.yaml`](config.yaml) - Batch configuration
- [`README.md`](README.md) - This file
