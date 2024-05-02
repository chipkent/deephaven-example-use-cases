
""" A package that provides a CombinedTable class that combines a historical and live table into a single table."""

from typing import Union, Sequence, Optional, Callable
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

    def __init__(self,
                 hist: Table,
                 live: Table,
                 hist_filters: Sequence[str] = None,
                 live_filters: Sequence[str] = None,
                 ):
        """ Create a CombinedTable object.

        Args:
            hist: The historical table.
            live: The live table.
            hist_filters: The filters to apply to the historical table.
            live_filters: The filters to apply to the live table.
        """
        self._historical_raw = hist
        self._live_raw = live
        self._hist_filters = hist_filters
        self._live_filters = live_filters
        self._historical = None
        self._live = None
        self._combined = None

    @property
    def historical(self) -> Table:
        """ The historical table."""
        if not self._historical:
            self._historical = self._historical_raw.where(self._hist_filters) \
                if self._hist_filters else self._historical_raw

        return self._historical

    @property
    def live(self) -> Table:
        """ The live table."""
        if not self._live:
            self._live = self._live_raw.where(self._live_filters) \
                if self._live_filters else self._live_raw

        return self._live

    @property
    def combined(self) -> Table:
        """ The combined table."""
        if not self._combined:
            self._combined = merge([self.historical, self.live])

        return self._combined

    @property
    def apply(self) -> 'CombinedTable':
        """ The CombinedTable with the filters fully applied."""
        return CombinedTable(self.historical, self.live)

    @staticmethod
    def _args_to_table_decorator(fn: Callable) -> Callable:
        """ Decorator to convert function arguments from CombinedTable to Table objects."""
        def wrapper(*args, **kwargs):
            # TODO: remove print statements
            print(f"WRAPPER: {args}")
            args_new = [arg.combined if isinstance(arg, CombinedTable) else arg for arg in args]
            return fn(*args_new, **kwargs)

        return wrapper

    # If the method doesn't exist in this object, delegate it to the combined table.
    # Methods are called with other CombinedTable objects as arguments, the arguments are converted to Table objects.
    def __getattr__(*args):
        print(f"GETATTR: {args}")
        obj = args[0]
        fn = args[1]
        c = obj.combined
        f = getattr(c, fn)
        return CombinedTable._args_to_table_decorator(f)

    @staticmethod
    def _combine_filters(filter1: Sequence[str], filter2: Sequence[str]) -> Optional[Sequence[str]]:
        """ Combine two sets of filters. If either filter is None, the other filter is returned."""
        if not filter1 and not filter2:
            return None
        elif not filter1:
            return filter2
        elif not filter2:
            return filter1
        else:
            return list(filter1) + list(filter2)

    def where(self,
              filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None,
              apply: bool = True,
              ) -> 'CombinedTable':
        """Applies the :meth:`~Table.where` table operation to the combined table, and produces a new CombinedTable.

        Args:
            filters (Union[str, Filter, Sequence[str], Sequence[Filter]], optional): the filter condition
                expression(s) or Filter object(s), default is None
            apply (bool, optional): whether to immediately apply the filters or defer the application until later,
                default is True

        Returns:
            a new CombinedTable object

        Raises:
            DHError
        """

        print("DELEGATED: where")

        if not filters:
            return self

        if isinstance(filters, str) or isinstance(filters, Filter):
            filters = [filters]

        has_filter = any(isinstance(f, Filter) for f in filters)

        if has_filter:
            return CombinedTable(
                self.historical.where(filters),
                self.live.where(filters),
            )

        hist_filters = self._combine_filters(self._hist_filters, filters)
        live_filters = self._combine_filters(self._live_filters, filters)

        if apply:
            return CombinedTable(
                self._historical_raw.where(hist_filters) if hist_filters else self._historical_raw,
                self._live_raw.where(live_filters) if live_filters else self._live_raw,
            )
        else:
            return CombinedTable(
                self._historical_raw,
                self._live_raw,
                hist_filters=hist_filters,
                live_filters=live_filters,
            )

    def where_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> 'CombinedTable':
        """Applies the :meth:`~Table.where_in` table operation to the combined table,
        and produces a new CombinedTable."""
        print("DELEGATED: where_in")
        return CombinedTable(
            self.historical.where_in(filter_table, cols),
            self.live.where_in(filter_table, cols)
        )

    def where_not_in(self, filter_table: Table, cols: Union[str, Sequence[str]]) -> 'CombinedTable':
        """Applies the :meth:`~Table.where_not_in` table operation to the combined table,
        and produces a new CombinedTable."""
        print("DELEGATED: where_not_in")
        return CombinedTable(
            self.historical.where_not_in(filter_table, cols),
            self.live.where_not_in(filter_table, cols)
        )

    def where_one_of(self, filters: Union[str, Filter, Sequence[str], Sequence[Filter]] = None) -> 'CombinedTable':
        """Applies the :meth:`~Table.where_one_of` table operation to the combined table,
        and produces a new CombinedTable."""
        print("DELEGATED: where_one_of")
        return CombinedTable(
            self.historical.where_one_of(filters),
            self.live.where_one_of(filters)
        )


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
    return CombinedTable(hist, live, hist_filters=["Date < date"], live_filters=["Date = date"])


ct = combined_table("FeedOS", "EquityQuoteL1")
ct_live = ct.live
ct_hist = ct.historical
ct_comb = ct.combined
# print(ct.is_replay) # Method does not exist
print(ct.is_blink)
f1 = ct.where("Date > `2024-04-09`")
fc1 = f1.combined
h1 = f1.head(3)

f2 = f1.where("Date < `2024-04-11`")
fc2 = f2.combined
h2 = f2.head(3)
