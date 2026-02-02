# Real-time AI Integration with Table Listeners

This example demonstrates how to integrate external AI/ML libraries (like NumPy, TensorFlow, or PyTorch) into a real-time Deephaven pipeline using **Table Listeners**.

## Overview

The script [`listener_based_ai.py`](./listener_based_ai.py) shows how to:
1.  **Listen** to a changing input table (`TableListener`).
2.  **Extract** data updates (added or modified rows).
3.  **Process** the data using an external function (simulated here with NumPy, but could be a complex model inference).
4.  **Publish** the results back into a new Deephaven table.

## Key Components

*   **`time_table`**: Generates a simulated stream of input data.
*   **`table_publisher`**: Creates a destination table for the AI model predictions.
*   **`listen`**: The core function that triggers the `on_update` callback whenever the source table changes.
*   **`on_update`**: The callback function handling data extraction, model inference, and result publishing.

## Running the Example

Copy the contents of the script [`listener_based_ai.py`](./listener_based_ai.py) and paste it directly into the Deephaven console to run it.

## Output

*   **`source`**: The input table generating random labels and values.
*   **`AIOutput`**: The real-time table containing the "predictions" (sum of inputs) from the external model.
