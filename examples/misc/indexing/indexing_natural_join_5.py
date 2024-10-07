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

t =  empty_table(100_000_000).update(["I = ii % 100", "J = ii % 7", "V = random()"]).sort(["I","J"])
t2 = t.last_by(["I", "J"])

idx = has_data_index(t, ["I", "J"])
idx2 = has_data_index(t2, ["I", "J"])
print(f"Has index: {idx} {idx2}")
time_it("without index", lambda : t.natural_join(t2, ["I", "J"], "VJ = V"))

print("Adding data index")
time_it("adding index", lambda : data_index(t, ["I", "J"]).table)
print("Adding data index 2")
time_it("adding index 2", lambda : data_index(t2, ["I", "J"]).table)

idx = has_data_index(t, ["I", "J"])
idx2 = has_data_index(t2, ["I", "J"])
print(f"Has index: {idx} {idx2}")
time_it("with index", lambda : t.natural_join(t2, ["I", "J"], "VJ = V"))
