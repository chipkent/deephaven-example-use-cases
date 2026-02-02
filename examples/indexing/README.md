# Indexing Performance Benchmark

This example demonstrates how to evaluate the performance benefits of using indices in Deephaven queries.

The script [`indexing.py`](./indexing.py) runs a series of benchmarks comparing query performance under different conditions:
1.  **Index Presence**: Comparing queries with and without a defined index.
2.  **Data Organization**: Comparing queries on fragmented (unsorted) vs. unfragmented (sorted) data.

## Key Concepts

*   **`deephaven.experimental.data_index`**: The module used to create and manage indices.
*   **Performance Impact**: Indices significantly speed up operations like `where`, `count_by`, `sum_by`, and `natural_join`, especially on large datasets.
*   **Data Fragmentation**: Sorting data (clustering) can further improve performance by improving data locality, even without an explicit index.

## Running the Example

Copy the contents of the script [`indexing.py`](./indexing.py) and paste it directly into the Deephaven console to run it.

## Benchmarks Performed

The script runs 6 cases:
1.  **Baseline**: Unsorted data, No index.
2.  **Index on (I, J)**: Unsorted data, Single composite index.
3.  **Indices on I and J**: Unsorted data, Separate indices.
4.  **Sorted**: Data sorted by (I, J), No index.
5.  **Sorted + Index**: Data sorted, Composite index.
6.  **Sorted + Separate Indices**: Data sorted, Separate indices.

For each case, it measures the time taken for:
*   `where` filtering
*   `count_by` aggregation
*   `sum_by` aggregation
*   `natural_join` operation
