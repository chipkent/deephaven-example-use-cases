import os
from deephaven import new_table
from deephaven.column import string_col, int_col
from deephaven.time import dh_today

simulation_name = os.getenv("SIMULATION_NAME")
simulation_date = os.getenv("SIMULATION_DATE")
worker_id = int(os.getenv("WORKER_ID", "0"))
num_workers = int(os.getenv("NUM_WORKERS", "1"))
output_table = os.getenv("OUTPUT_TABLE", "worker_results")
log_level = os.getenv("LOG_LEVEL", "INFO")

print(f"[{log_level}] Simple Worker Started")
print(f"[{log_level}] Simulation Name: {simulation_name}")
print(f"[{log_level}] Simulation Date: {simulation_date}")
print(f"[{log_level}] dh_today(): {dh_today()}")
print(f"[{log_level}] Worker ID: {worker_id}")
print(f"[{log_level}] Number of Workers: {num_workers}")
print(f"[{log_level}] Output Table: {output_table}")

worker_status = new_table([
    string_col("Date", [simulation_date]),
    int_col("WorkerID", [worker_id]),
    int_col("NumWorkers", [num_workers]),
    string_col("Status", ["COMPLETED"]),
    string_col("Message", [f"Worker {worker_id} processed date {simulation_date}"])
])

print(f"[{log_level}] Simple Worker Completed Successfully")
