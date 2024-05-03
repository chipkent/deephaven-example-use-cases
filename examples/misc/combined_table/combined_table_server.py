
""" A package that provides a CombinedTable class for the deephaven server
that combines a historical and live table into a single table."""

from combined_table_common import CombinedTable

from deephaven_enterprise.database import db
from deephaven import merge
from deephaven.time import dh_today

def combined_table(namespace: str, table_name: str) -> CombinedTable:
    """ Create a combined table for the given namespace and table name.

    The live table is for today according to deephaven.time.dh_today().
    The historical table is for all dates less than today.

    Args:
        namespace: The namespace of the table.
        table_name: The name of the table.

    Returns:
        A CombinedTable object.
    """
    # noinspection PyUnusedLocal
    date = dh_today()
    hist = db.historical_table(namespace, table_name)
    live = db.live_table(namespace, table_name)
    return CombinedTable(merge, hist, live, hist_filters=["Date < date"], live_filters=["Date = date"])

