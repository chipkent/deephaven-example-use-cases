"""
Cleanup Script for Trading Simulation Batch

Deletes all user tables created by the trading simulation batch orchestration.
Designed to be run via the PQ orchestrator using cleanup.yaml.
"""

import os

# Get namespace from environment (required)
output_namespace = os.getenv("OUTPUT_NAMESPACE")
if not output_namespace:
    raise ValueError("OUTPUT_NAMESPACE environment variable is required")

# Table names created by the trading simulation
TABLE_NAMES = [
    "TradingSimTrades",
    "TradingSimPositions",
    "TradingSimPnl",
    "TradingSimPreds",
    "TradingSimSummary"
]

print(f"[INFO] Cleanup Script Started")
print(f"[INFO] Target Namespace: {output_namespace}")

deleted_count = 0
error_count = 0

for table_name in TABLE_NAMES:
    try:
        db.delete_partitioned_table(output_namespace, table_name)
        print(f"[INFO] Deleted partitioned table: {table_name}")
        deleted_count += 1
    except Exception as e:
        # Table may not exist, which is fine
        print(f"[WARN] Could not delete {table_name}: {e}")
        error_count += 1

print(f"[INFO] Cleanup Complete: {deleted_count} deleted, {error_count} errors/not found")
