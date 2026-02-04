"""
This script runs on the 'Client' instance (port 10001).
It acts as a consumer, fetching tables from the 'Source' instance (port 10000) using URI resolution.
"""
from deephaven.uri import resolve

# ------------------------------------------------------------------------------
# Configuration: Authentication
# The server is configured with Anonymous authentication because Deephaven Core
# currently requires it for URI resolution (see issue #5383).
# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
# 1. Resolve Remote Tables
# We use the 'dh+plain' scheme for an insecure connection to localhost.
# Format: dh+plain://<host>:<port>/scope/<table_name>
# ------------------------------------------------------------------------------

raw_metrics = resolve("dh+plain://localhost:10000/scope/raw_metrics")
high_value_metrics = resolve("dh+plain://localhost:10000/scope/high_value_metrics")
avg_by_id = resolve("dh+plain://localhost:10000/scope/avg_by_id")

print("Resolved tables: 'raw_metrics', 'high_value_metrics', 'avg_by_id'.")

# ------------------------------------------------------------------------------
# 2. Perform Client-Side Operations
# These operations run on THIS 'Client' instance, using the data stream from the server.
# ------------------------------------------------------------------------------

# Filter locally
filtered_client_view = avg_by_id.where("AvgValue > 50")
print("Created 'filtered_client_view' (Local Filter).")

# Join locally: combining the high value metrics with another remote table
joined_client_view = high_value_metrics.natural_join(
    avg_by_id, 
    on=["MetricId"], 
    joins=["AvgValue"]
)
print("Created 'joined_client_view' (Local Join).")

print("Script complete. Tables are ready in the variable explorer.")
