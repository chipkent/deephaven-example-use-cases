
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

n = 100    

t =  empty_table(100_000_000).update(["I = ii % n", "V = random()"])
t2 = t.last_by("I")

idx = has_data_index(t2, ["I"])
print(f"Has index: {idx}")
time_it("without index", lambda : t.natural_join(t2, "I", "VJ = V"))

print("Adding data index")
time_it("adding index", lambda : data_index(t2, ["I"]).table)

idx = has_data_index(t2, ["I"])
print(f"Has index: {idx}")
time_it("with index", lambda : t.natural_join(t2, "I", "VJ = V"))


