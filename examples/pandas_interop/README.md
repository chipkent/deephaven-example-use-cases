# Pandas Interoperability Basics

This directory contains examples demonstrating how to exchange data between Deephaven tables and Pandas DataFrames.

## Overview

Deephaven and Pandas integrate seamlessly, but they operate on different paradigms: Deephaven is streaming and update-oriented, while Pandas is static and batch-oriented. These examples show two strategies for keeping a Pandas DataFrame synchronized with a live Deephaven table.

## Examples

### 1. Listener Approach (Push-based)
**File**: [`update_dataframe_listener.py`](./update_dataframe_listener.py)

This example uses a **Table Listener** (`deephaven.table_listener`). 
*   **Mechanism**: It registers a callback function that is invoked by the Deephaven engine every time the source table updates.
*   **Pros**: Efficient and reactive. Updates happen as soon as data changes.
*   **Cons**: The callback runs on the update thread, so heavy processing here can block the update stream.

### 2. Thread Approach (Pull-based)
**File**: [`update_dataframe_thread.py`](./update_dataframe_thread.py)

This example uses a **background Python thread**.
*   **Mechanism**: A separate thread wakes up at a set interval (e.g., every 5 seconds) and manually snapshots the Deephaven table into a DataFrame.
*   **Pros**: Decoupled execution. The update logic runs completely independently of the Deephaven engine's update cycle.
*   **Cons**: Less efficient (polling) and updates are not real-time.

## Running the Examples

Run these scripts directly in the Deephaven console.

**Listener Example:**
```python
exec(open("examples/pandas_interop/update_dataframe_listener.py").read())
```
*Watch the "Update: Value:..." output printed to the console as the table updates.*

**Thread Example:**
```python
exec(open("examples/pandas_interop/update_dataframe_thread.py").read())
```
*Watch the background thread print the DataFrame value every 5 seconds.*
