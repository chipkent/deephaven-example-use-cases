
""" Examine indexing and query performance. 

The following benchmarks look at indexing performance under a few conditions.
1. Without an index vs with an index. 
2. Data is fragmented (unsorted) vs unfragmented (sorted).

Fragmented data can result in non-local data access patterns that result in worse performance.
"""

from time import time
from deephaven import empty_table
from deephaven.experimental.data_index import data_index, has_data_index
import jpy

# Disable memoization to obtain accurate performance results
jpy.get_type("io.deephaven.engine.table.impl.QueryTable").setMemoizeResults(False)

def time_it(name, f):
    """ Time a function execution. """
    start = time()
    f()
    end = time()
    print(f"Executed {name} in {(end-start)} sec")

# Generate a table to analyze.  t is fragmented in the key columns ["I", "J"]
t1 =  empty_table(100_000_000).update(["I = ii % 100", "J = ii % 7", "V = random()"])
t2 = t1.last_by(["I", "J"])

def add_index(t):
    """ Add a data index.  By adding .table, the index calculation is forced to be now instead of when it is first used. """
    print("Adding data index")
    time_it("adding index", lambda : data_index(t, ["I", "J"]).table)

def run_test(t):
    """ Runs a series of performance benchmarks. """
    idx = has_data_index(t, ["I", "J"])
    print(f"Has index: {idx}")
    time_it("where", lambda : t.where(["I = 3", "J = 6"]))
    time_it("count_by", lambda : t.count_by("Count", ["I", "J"]))
    time_it("sum_by", lambda : t.sum_by(["I", "J"]))
    time_it("natural_join", lambda : t.natural_join(t2, ["I", "J"], "VJ = V"))

print("\n*** CASE 1: sorting=False index=False ***")
run_test(t1)

print("\n*** CASE 2: sorting=False index=True ***")
add_index(t1)
run_test(t1)

print("\n*** CASE 3: sorting=True index=False ***")
t3 = t1.sort(["I", "J"])
run_test(t3)

print("\n*** CASE 4: sorting=True index=True ***")
add_index(t3)
run_test(t3)
