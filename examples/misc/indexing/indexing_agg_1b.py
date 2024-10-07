
from time import time
from deephaven import empty_table
from deephaven.experimental.data_index import data_index, has_data_index
import jpy

jpy.get_type("io.deephaven.engine.table.impl.QueryTable").setMemoizeResults(False)

def time_it(name, f):
    start = time()
    f()
    end = time()
    print(f"Executed {name} in {(end-start)} sec")

t =  empty_table(100_000_000).update(["I = ii % 100", "J = ii % 7", "V = random()"]).sort(["I", "J"])

idx = has_data_index(t, ["I", "J"])
print(f"Has index: {idx}")
time_it("without index", lambda : t.sum_by(["I", "J"]))

print("Adding data index")
time_it("adding index", lambda : data_index(t, ["I", "J"]).table)

idx = has_data_index(t, ["I", "J"])
print(f"Has index: {idx}")
time_it("with index", lambda : t.sum_by(["I", "J"]))


