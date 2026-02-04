# Sharing Tables (Client-Server) Example

This example demonstrates a fundamental distributed computing pattern in Deephaven: **publishing** tables on one server and **consuming** them from another using [URI Resolution](https://deephaven.io/core/docs/conceptual/uri-resolution/).

## Overview

In this example, we simulate a distributed system with two independent Deephaven instances:

1.  **Source Server (Port 10000)**: Acts as the data producer. It generates real-time data and publishes it.
2.  **Client Server (Port 10001)**: Acts as the consumer. It connects to the Source Server to fetch tables and perform further analysis locally.

## Key Concepts

*   **[URI Resolution](https://deephaven.io/core/docs/conceptual/uri-resolution/)**: The mechanism used to fetch disjoint tables from a remote server using a standardized URI format (e.g., `dh+plain://host:port/scope/table`).
    *   **Community Core Note**: This example uses **Anonymous Authentication** because Deephaven Core currently only supports anonymous authentication for URI resolution (see issues [#5383](https://github.com/deephaven/deephaven-core/issues/5383) and [#3421](https://github.com/deephaven/deephaven-core/issues/3421)).
    *   **Enterprise Note**: In **Deephaven Enterprise**, authentication is managed automatically and seamlessly, so this limitation does not apply.
*   **[Time Table](https://deephaven.io/core/docs/reference/table-operations/source/time-table/)**: Used on the source server to generate a ticking data stream.
*   **[Aggregations](https://deephaven.io/core/docs/reference/table-operations/aggregation/agg-by/)**: Used to compute real-time statistics (`avg_by`) on the source.
*   **[Natural Join](https://deephaven.io/core/docs/reference/table-operations/join/natural-join/)**: Used on the client to combine a resolved remote table with another local view.

## Files

*   **[`setup.sh`](./setup.sh)**: Sets up the Python environment (Prerequisite).
*   **[`run.sh`](./run.sh)**: Orchestration script. Launches both servers.
*   **[`server.py`](./server.py)**: The producer logic. Generates data and aggregations.
*   **[`client.py`](./client.py)**: The consumer logic. Resolves remote tables and joins them.

## Prerequisites

This example requires Docker and Python. The setup script will create a Python virtual environment and install the necessary `deephaven-server` package.

```bash
./setup.sh
```

## Running the Example

### 1. Start the Servers
Execute the run script to launch both Deephaven instances. This will open two browser tabs automatically.

```bash
./run.sh
```

*   **Tab 1 (http://localhost:10000)**: The **Source** Server
*   **Tab 2 (http://localhost:10001)**: The **Client** Server

### 2. Configure the Source (Tab 1)
Switch to the **Source Server** tab (Port 10000).

1.  Copy the code from **[`server.py`](./server.py)**.
2.  Paste it into the script console and run it.
3.  **Verify**: You should see three tables appear in the "Panels" area: `raw_metrics`, `high_value_metrics`, and `avg_by_id`. These are now "published" and available for resolution.

### 3. Configure the Client (Tab 2)
Switch to the **Client Server** tab (Port 10001).

1.  Copy the code from **[`client.py`](./client.py)**.
2.  Paste it into the script console and run it.
3.  **Verify**:
    *   The script uses `deephaven.uri.resolve` to fetch the distinct tables from Port 10000.
    *   New tables will appear: `raw_metrics`, `high_value_metrics`, and `avg_by_id` (these are proxies to the source).
    *   Derived tables `filtered_client_view` and `joined_client_view` are created locally, demonstrating that you can join and filter remote tables just like local ones.

## Need Help?

*   [Deephaven Documentation](https://deephaven.io/core/docs/)
*   [Join the Slack Community](https://deephaven.io/slack)
