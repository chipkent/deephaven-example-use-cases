# Simple Worker Batch Example

A minimal batch mode example demonstrating the replay orchestrator framework with non-replay persistent queries.

## Overview

This example creates batch (RunAndDone) persistent queries that process historical data without replay functionality. It's designed to verify the batch orchestration infrastructure works correctly.

## What It Does

Creates a simple status table showing which worker processed which date and partition. The query runs once and terminates automatically.

## Key Differences from Replay Version

- Uses `batch:` configuration section instead of `replay:`
- Configured with `timeout_minutes` instead of `init_timeout_minutes`
- No `stop_and_wait()` call (RunAndDone auto-terminates)
- Persistent queries named `batch_*` instead of `replay_*`

## Configuration

**Scale**: 2 partitions per date × 5 weekdays (Jan 1-5, 2024) = 10 sessions

**File**: [`config.yaml`](config.yaml)

Key settings:

- `batch.timeout_minutes: 5` - Maximum runtime for each batch query
- `execution.heap_size_gb: 4.0` - Memory allocation per query
- `execution.script_language: "Python"` - Worker script language

## Running

Set environment variables:

```bash
export DH_CONNECTION_URL="https://your-server:8000/iris/connection.json"
export DH_USERNAME="your_username"
export DH_PASSWORD="your_password"
```

Run the orchestrator:

```bash
cd /path/to/replay_orchestration
replay-orchestrator --config simple_worker_batch/config.yaml
```

## Output

Each worker creates a `worker_status` table with:

- Date processed
- Partition ID
- Status (COMPLETED)
- Custom message from configuration

## Files

- [`simple_worker_batch.py`](simple_worker_batch.py) - Worker script
- [`config.yaml`](config.yaml) - Batch configuration
- [`README.md`](README.md) - This file
