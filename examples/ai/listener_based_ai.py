# This example illustrates how Deephaven table listeners can be used to drive real-time AI calculations
#
# In this case, a simple NumPy calculation is used for the AI model, but TensorFlow, PyTorch, or any 
# other AI technologies could be used.

# Imports

import numpy as np
import random
from deephaven import time_table, dtypes, new_table
from deephaven.column import datetime_col, string_col, double_col
from deephaven.table_listener import listen, TableUpdate
from deephaven.stream import blink_to_append_only
from deephaven.stream.table_publisher import table_publisher

# Create a table to be used as input

def gen_label() -> str:
    return random.choice(["Denver", "New York", "Chicago", "Boise"])

source = time_table("PT0.1S").update(formulas=["Label = gen_label()", "X = sqrt(i)", "Y = -sqrt(sqrt(i))"])

# Set up the output tables

preds, preds_publisher = table_publisher("AIOutput", {"Timestamp": dtypes.Instant, "Label": dtypes.string, "Pred": dtypes.float64})

preds_history = blink_to_append_only(preds)

# Define the AI model

def compute_ai_model(features):
    return np.sum(features, axis=1, keepdims=True)

# Create a table listener

def on_update(update: TableUpdate, is_replay: bool) -> None:
    cols = ["Timestamp", "Label", "X", "Y"]
    adds = update.added(cols)
    modifies = update.modified(cols)

    # Combine adds and modifies

    data = {}

    for col in cols:
        if adds and modifies:
            data[col] = np.hstack(adds[col], modifies[col])
        elif adds:
            data[col] = adds[col]
        elif modifies:
            data[col] = modifies[col]
        else:
            return

    # Create the input to the AI model

    inputs = np.stack([data[k] for k in ["X", "Y"]], axis=-1)

    # Compute the AI model

    outputs = compute_ai_model(inputs)

    # Populate the output table

    tout = new_table([
        datetime_col("Timestamp", data["Timestamp"]),
        string_col("Label", data["Label"]),
        double_col("Pred", outputs[:, 0]),
    ])

    preds_publisher.add(tout)


handle = listen(source, on_update, do_replay=True)

