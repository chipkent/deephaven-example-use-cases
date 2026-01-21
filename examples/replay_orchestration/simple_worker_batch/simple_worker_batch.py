import os
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.time import dh_today

simulation_name = os.getenv("SIMULATION_NAME")
simulation_date = os.getenv("SIMULATION_DATE")
partition_id = int(os.getenv("PARTITION_ID"))
num_partitions = int(os.getenv("NUM_PARTITIONS"))
log_level = os.getenv("LOG_LEVEL", "INFO")
custom_message = os.getenv("CUSTOM_MESSAGE")

print(f"[{log_level}] Simple Worker Batch Started")
print(f"[{log_level}] Simulation Name: {simulation_name}")
print(f"[{log_level}] Simulation Date: {simulation_date}")
print(f"[{log_level}] dh_today(): {dh_today()}")
print(f"[{log_level}] Partition ID: {partition_id}")
print(f"[{log_level}] Number of Partitions: {num_partitions}")
print(f"[{log_level}] Custom Message: {custom_message}")

worker_status = new_table([
    string_col("Date", [simulation_date]),
    int_col("PartitionID", [partition_id]),
    int_col("NumPartitions", [num_partitions]),
    string_col("Status", ["COMPLETED"]),
    string_col("Message", [f"Partition {partition_id} processed date {simulation_date}"]),
    string_col("CustomMessage", [custom_message])
])

print(f"[{log_level}] Simple Worker Batch Completed Successfully")
