# Simple Worker Example

This is a minimal example worker script to **verify the replay orchestration framework is working correctly**.

## Purpose

This example is designed to:

- **Test the orchestrator**: Verify sessions are created and executed successfully
- **Validate environment variables**: Confirm all expected variables are passed correctly
- **Demonstrate basic patterns**: Show how to read config values and use auto-generated variables
- **Print diagnostic output**: Display values to console for verification

**Note**: This example does **not** persist any data. It creates an in-memory table and prints values to demonstrate the orchestrator is working. For examples that write to shared tables, see the trading_simulation example.

## Environment Variables

The orchestrator automatically provides:

- `SIMULATION_NAME`: Unique simulation identifier from config top-level `name` field
- `SIMULATION_DATE`: The date being processed (YYYY-MM-DD) - also available via [`dh_today()`](https://docs.deephaven.io/core/pydoc/code/deephaven.time.html#deephaven.time.dh_today)
- `WORKER_ID`: The worker's ID (0 to NUM_WORKERS-1) for partitioning data
- `NUM_WORKERS`: Total number of workers per date

From `config.yaml` env section:

- `OUTPUT_TABLE`: Name of the output table
- `LOG_LEVEL`: Logging level

## Configuration

See `config.yaml` for the complete orchestrator configuration. Key settings:

```yaml
execution:
  num_workers: 2              # Creates 2 workers per date
  
dates:
  start: "2024-01-01"
  end: "2024-01-05"
  weekdays_only: true         # Only Mon-Fri (3 days in this range)
```

This creates 2 workers per date × 3 dates = 6 total sessions.

## Running

From the `replay_orchestration` directory:

```bash
replay-orchestrator --config simple_worker/config.yaml
```

## Output

Each session prints to console:

- All environment variables (SIMULATION_DATE, WORKER_ID, NUM_WORKERS, custom vars)
- Comparison of SIMULATION_DATE vs `dh_today()` to verify replay date
- Status messages confirming execution

The script also creates an in-memory table (`worker_status`) to demonstrate basic Deephaven table creation, but **does not publish or persist it**. This is intentional - the example is purely for verification.

## Verification

After running, check the orchestrator console output to verify:

- All 6 sessions (2 workers × 3 dates) were created
- Each session completed successfully
- No failures reported
- Exit code 0 (success)
