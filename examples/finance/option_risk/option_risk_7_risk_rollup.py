""" Compute hierarchical risk aggregation (rollup) for drill-down analysis.

A rollup table provides multi-level aggregation that allows users to:
- View total risk across the entire portfolio
- Drill down by underlying symbol (USym)
- Further drill down by expiration date (Expiry)
- Further drill down by strike price (Strike)
- View individual option contracts (Parity: CALL/PUT)

Hierarchy levels:
1. **Portfolio Total** - All risk metrics summed
2. **By Symbol** - Risk aggregated per underlying (e.g., all AAPL positions)
3. **By Expiry** - Risk per symbol and expiration date
4. **By Strike** - Risk per symbol, expiry, and strike price
5. **By Parity** - Individual CALL or PUT contract level

This enables portfolio managers to:
- Quickly identify which symbols have the most exposure
- See concentration risk at different levels
- Navigate from high-level totals to individual positions

All numeric risk metrics (Theo, DollarDelta, Gamma, etc.) are summed at each level.
"""

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
