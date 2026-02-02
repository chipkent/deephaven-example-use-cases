# Combined Historical and Live Table

This utility provides a `CombinedTable` class that seamlessly merges a historical table (usually on disk) with a live table (in memory) into a single, queryable interface.

## Why Use This?

In many real-time applications, you have massive historical data stored on disk and a smaller stream of real-time data in memory. Merging them naively (e.g., `merge([hist, live])`) works, but it can be inefficient for queries.

The `CombinedTable` optimizes performance by **pushing filters down** to the source tables *before* the merge.

### Performance Benefit
If you run:
```python
t = merge([hist, live])
res = t.where("Symbol = 'AAPL'")
```
The engine might have to scan the entire combined result.

With `CombinedTable`:
```python
t = CombinedTable(merge, hist, live)
res = t.where("Symbol = 'AAPL'")
```
It intelligently translates this into:
1.  `hist.where("Symbol = 'AAPL'")` (Fast, indexed disk scan)
2.  `live.where("Symbol = 'AAPL'")` (Fast memory scan)
3.  `merge([filtered_hist, filtered_live])`

This keeps the historical and live tables separate as long as possible, maintaining indexing and partitioning benefits.

## Usage

### Server-Side
Use `combined_table_server` when running scripts on the Deephaven server.

```python
from examples.misc.combined_table.combined_table_server import combined_table

# Create a combined view of the "trades" table
# Assumes you have a "trades" table in both historical and live databases
trades = combined_table("data", "trades", date_col="Date")

# Use it exactly like a normal table
aapl_trades = trades.where(["Symbol = 'AAPL'", "Price > 150"])
```

### Client-Side (Python Client)
Use `combined_table_client` when connecting from a Python client.

```python
from examples.misc.combined_table.combined_table_client import combined_table

# session is your pydeephaven.Session object
trades = combined_table(session, "data", "trades", date_col="Date")

# Apply filters naturally
high_val_trades = trades.where("Quantity * Price > 10000")
```

## Files

*   **[`combined_table_common.py`](./combined_table_common.py)**: The core logic class.
*   **[`combined_table_server.py`](./combined_table_server.py)**: Factory function for server-side usage.
*   **[`combined_table_client.py`](./combined_table_client.py)**: Factory function for client-side usage.
*   **[`test_server.py`](./test_server.py)**: Example script for server-side.
*   **[`test_client.py`](./test_client.py)**: Example script for client-side.