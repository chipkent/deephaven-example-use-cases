
# An example of using a deephaven table listener to update a dataframe

import threading
import time
from deephaven import time_table
from deephaven.pandas import to_pandas
from deephaven.table_listener import listen

t = time_table("PT1s").update("X=ii").tail(5)
tsnap = t.snapshot_when(time_table("PT5s").view("TSnap=Timestamp"))
df = to_pandas(tsnap)

def update_df(update, is_replay):
    global df
    df = to_pandas(tsnap)
    print(f"Update: Value: {df}")

handle = listen(tsnap, update_df)

def terminate_listener():
    print("Terminating listener...")
    handle.stop()
    print("Listener has terminated.")

threading.Timer(20, terminate_listener)


