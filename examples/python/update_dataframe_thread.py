
# An example of using python threads and deephaven to update a dataframe

import threading
import time
from deephaven import time_table
from deephaven.pandas import to_pandas

t = time_table("PT1s").update("X=ii").tail(5)
df = to_pandas(t)

def update_df(t, interval):
    global df
    while True:
        df = to_pandas(t)
        print(f"Thread: {threading.current_thread().name}, Value: {df}")
        time.sleep(interval)

# Create and start the thread
thread = threading.Thread(target=update_df, args=(t, 5), name="UpdateThread")
thread.start()

# Let the thread run for 20 seconds
time.sleep(20)

# Terminate the thread (in this case, forcefully)
print("Terminating thread...")
thread.join(timeout=1)
if thread.is_alive():
    print("Thread is still running. Exiting anyway.")
else:
    print("Thread has terminated.")
