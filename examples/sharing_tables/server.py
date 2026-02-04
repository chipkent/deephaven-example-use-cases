"""
This script runs on the 'Server' instance (port 10000).
It acts as the data producer, generating live streams and derived tables.
"""
from deephaven import time_table
from deephaven import agg

print("Creating tables on the server...")

# ------------------------------------------------------------------------------
# 1. Create a Live Table
# 'raw_metrics' ticks every second to simulate a real-time data feed.
# ------------------------------------------------------------------------------

raw_metrics = time_table("PT1S").update([
    "MetricId = i % 5",
    "Value = randomDouble(0, 100)"
])

# ------------------------------------------------------------------------------
# 2. Create Derived Tables (Views)
# These operations run on the server and are updated in real-time.
# ------------------------------------------------------------------------------

# Filter: Create a view of high-value metrics
high_value_metrics = raw_metrics.where("Value > 80")

# Aggregation: Maintain the latest average value for each MetricId
avg_by_id = raw_metrics.agg_by(agg.avg("AvgValue = Value"), by=["MetricId"])

print("Tables created: 'raw_metrics', 'high_value_metrics', 'avg_by_id'.")
print("Server is ready.")
