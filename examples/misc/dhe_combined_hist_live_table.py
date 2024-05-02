
""" A package that provides a CombinedTable class that combines a historical and live table into a single table."""

from typing import Union, Sequence
from deephaven_enterprise.database import db
from deephaven import merge
from deephaven.table import Table, Filter
from deephaven.time import dh_today

class CombinedTable:
    """ A class that combines a historical and live table into a single table.
    This class is used to provide a single interface to both tables.

    Overrides of table methods are provided to optimize performance.
    If a performance optimizing override is not provided, the method call is delegated to the combined table.
    """
    def __init__(self, hist: Table, live: Table):
        self._historical = hist
        self._live = live
        self._combined = None

    @property
    def historical(self):
        """ The historical table."""
        return self._historical

    @property
    def live(self):
        """ The live table."""
        return self._live

    @property
    def combined(self):
        """ The combined table."""
        if not self._combined:
            self._combined = merge([self._historical, self._live])

        return self._combined

    # If the method doesn't exist in this object, delegate it to the combined table.
    def __getattr__(*args):
        print(f"GETATTR: {args}")
        obj = args[0]
        fn = args[1]
        c = obj.combined
        return getattr(c, fn)

    def where(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None) -> 'CombinedTable':
        print("DELEGATED: where")
        return CombinedTable(
            self._historical.where(filters),
            self._live.where(filters)
        )

    def where_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> 'CombinedTable':
        print("DELEGATED: where_in")
        return CombinedTable(
            self._historical.where_in(filter_table, cols),
            self._live.where_in(filter_table, cols)
        )

    def where_not_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> 'CombinedTable':
        print("DELEGATED: where_not_in")
        return CombinedTable(
            self._historical.where_not_in(filter_table, cols),
            self._live.where_not_in(filter_table, cols)
        )

    def where_one_of(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None) -> 'CombinedTable':
        print("DELEGATED: where_one_of")
        return CombinedTable(
            self._historical.where_one_of(filters),
            self._live.where_one_of(filters)
        )


def combined_table(namespace:str, table_name:str) -> CombinedTable:
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
    hist = db.historical_table(namespace, table_name).where("Date < date")
    live = db.live_table(namespace, table_name).where("Date = date")
    return CombinedTable(hist, live)



ct = combined_table("FeedOS", "EquityQuoteL1")
live = ct.live
hist = ct.historical
comb = ct.combined
# print(ct.is_replay) # Method does not exist
print(ct.is_blink)
f = ct.where("Date > `2024-04-09`")
fc = f.combined
h = f.head(3)

f2 = f.where("Date < `2024-04-11`")
fc2 = f2.combined
h2 = f2.head(3)