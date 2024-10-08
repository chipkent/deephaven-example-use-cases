
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
    rst = f()
    end = time()
    print(f"Executed {name} in {(end-start)} sec")
    return rst

# Generate a table to analyze.  t is fragmented in the key columns ["I", "J"]
t1 =  empty_table(100_000_000).update(["I = ii % 100", "J = ii % 7", "V = random()"])
t2 = t1.last_by(["I", "J"])

def add_index(t, by):
    """ Add a data index.  By adding .table, the index calculation is forced to be now instead of when it is first used. """
    print(f"Adding data index: {by}")
    return time_it("adding index", lambda : data_index(t, by).table)

def run_test(t):
    """ Runs a series of performance benchmarks. """
    idx_ij = has_data_index(t, ["I", "J"])
    idx_i = has_data_index(t, ["I"])
    idx_j = has_data_index(t, ["J"])
    print(f"Has index: ['I', 'J']={idx_ij} ['I']={idx_i} ['J']={idx_j}")
    time_it("where", lambda : t.where(["I = 3", "J = 6"]))
    time_it("count_by", lambda : t.count_by("Count", ["I", "J"]))
    time_it("sum_by", lambda : t.sum_by(["I", "J"]))
    time_it("natural_join", lambda : t.natural_join(t2, ["I", "J"], "VJ = V"))

print("\n*** CASE 1: sorting=False index=False ***")
run_test(t1)

print("\n*** CASE 2: sorting=False index=True ***")
idx1_ij = add_index(t1, ["I", "J"])
run_test(t1)

print("\n*** CASE 3: sorting=False index=True ***")
idx1_i = add_index(t1, ["I"])
idx1_j = add_index(t1, ["J"])
run_test(t1)

print("\n*** CASE 4: sorting=True index=False ***")
t3 = t1.sort(["I", "J"])
run_test(t3)

print("\n*** CASE 5: sorting=True index=True ***")
idx3 = add_index(t3, ["I", "J"])
run_test(t3)

print("\n*** CASE 6: sorting=True index=True ***")
idx3_i = add_index(t3, ["I"])
idx3_j = add_index(t3, ["J"])
run_test(t3)
