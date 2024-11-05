
""" Compute the risk rollup for the given risk table. """

from deephaven import agg
from deephaven.table import Table, RollupTable


def compute_risk_rollup(risk_all: Table) -> RollupTable:
    """ Compute the risk rollup for the given risk table. """

    by = ["USym", "Expiry", "Strike", "Parity"]
    non_by = [col.name for col in risk_all.columns if col.name not in by]

    return risk_all \
        .rollup(
            aggs=[agg.sum_(non_by)],
            by=by,
            include_constituents=False,
        )
