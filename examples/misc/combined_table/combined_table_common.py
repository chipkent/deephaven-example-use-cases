
""" A package that provides a CombinedTable class that combines a historical and live table into a single table.

This class can be used with both the Deephaven client and server.
"""
import inspect
from typing import Union, Sequence, Optional, Callable, Generic, TypeVar

Table = TypeVar('Table')
""" The type of table.  
By making this a type variable, we can use the CombinedTable class on either the client or server."""


class CombinedTable(Generic[Table]):
    """ A class that combines a historical and live table into a single table.
    This class is used to provide a single interface to both tables.

    Overrides of table methods are provided to optimize performance.
    If a performance optimizing override is not provided, the method call is delegated to the combined table.
    """

    def __init__(self,
                 merge: Callable[[Sequence[Table]], Table],
                 hist: Table,
                 live: Table,
                 hist_filters: Sequence[str] = None,
                 live_filters: Sequence[str] = None,
                 ):
        """ Create a CombinedTable object.

        Args:
            merge: The function to merge tables.
            hist: The historical table.
            live: The live table.
            hist_filters: The filters to apply to the historical table.
            live_filters: The filters to apply to the live table.
        """
        self._merge = merge
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
            self._combined = self._merge([self.historical, self.live])

        return self._combined

    @property
    def apply(self) -> 'CombinedTable':
        """ The CombinedTable with the filters fully applied."""
        return CombinedTable(self._merge, self.historical, self.live)

    @staticmethod
    def _args_to_table_decorator(fn: Callable) -> Callable:
        """ Decorator to convert function arguments from CombinedTable to Table objects."""

        nargs = len(inspect.signature(fn).parameters)

        if nargs == 0:
            return fn

        def wrapper(*args, **kwargs):
            args_new = [arg.combined if isinstance(arg, CombinedTable) else arg for arg in args]
            return fn(*args_new, **kwargs)

        return wrapper

    # If the method doesn't exist in this object, delegate it to the combined table.
    # Methods are called with other CombinedTable objects as arguments, the arguments are converted to Table objects.
    def __getattr__(self, attr):
        c = self.combined
        a = getattr(c, attr)

        # This is to handle @property methods and properties
        if not callable(a):
            return a

        return CombinedTable._args_to_table_decorator(a)

    @staticmethod
    def _combine_filters(filters1: Union[str, Sequence[str]], filters2: Union[str, Sequence[str]]) -> Optional[Sequence[str]]:
        """ Combine two sets of filters. If either filter is None, the other filter is returned."""

        if isinstance(filters1, str):
            filters1 = [filters1]

        if isinstance(filters2, str):
            filters2 = [filters2]

        if not filters1 and not filters2:
            return None
        elif not filters1:
            return filters2
        elif not filters2:
            return filters1
        else:
            return list(filters1) + list(filters2)

    def where(self,
              filters: Union[str, Sequence[str]] = None,
              apply: bool = True,
              ) -> 'CombinedTable':
        """Applies the :meth:`~Table.where` table operation to the combined table, and produces a new CombinedTable.

        Args:
            filters (Union[str, Sequence[str]], optional): the filter condition
                expression(s), default is None
            apply (bool, optional): whether to immediately apply the filters or defer the application until later,
                default is True

        Returns:
            a new CombinedTable object

        Raises:
            DHError
        """

        if not filters:
            return self

        if apply:
            if self._historical:
                hist = self.historical.where(filters)
            else:
                hist_filters = self._combine_filters(self._hist_filters, filters)
                hist = self._historical_raw.where(hist_filters)

            if self._live:
                live = self.live.where(filters)
            else:
                live_filters = self._combine_filters(self._live_filters, filters)
                live = self._live_raw.where(live_filters)

            return CombinedTable(self._merge, hist, live)
        else:
            hist_filters = self._combine_filters(self._hist_filters, filters)
            live_filters = self._combine_filters(self._live_filters, filters)

            return CombinedTable(
                self._merge,
                self._historical_raw,
                self._live_raw,
                hist_filters=hist_filters,
                live_filters=live_filters,
            )

    def where_in(self, filter_table: Union[Table, 'CombinedTable'], cols: Union[str, Sequence[str]]) -> 'CombinedTable':
        """Applies the :meth:`~Table.where_in` table operation to the combined table,
        and produces a new CombinedTable."""

        if isinstance(filter_table, CombinedTable):
            filter_table = filter_table.combined

        return CombinedTable(
            self._merge,
            self.historical.where_in(filter_table, cols),
            self.live.where_in(filter_table, cols)
        )

    def where_not_in(self, filter_table: Union[Table, 'CombinedTable'], cols: Union[str, Sequence[str]]) -> 'CombinedTable':
        """Applies the :meth:`~Table.where_not_in` table operation to the combined table,
        and produces a new CombinedTable."""

        if isinstance(filter_table, CombinedTable):
            filter_table = filter_table.combined

        return CombinedTable(
            self._merge,
            self.historical.where_not_in(filter_table, cols),
            self.live.where_not_in(filter_table, cols)
        )

    def where_one_of(self, filters: Union[str, Sequence[str]] = None) -> 'CombinedTable':
        """Applies the :meth:`~Table.where_one_of` table operation to the combined table,
        and produces a new CombinedTable."""
        return CombinedTable(
            self._merge,
            self.historical.where_one_of(filters),
            self.live.where_one_of(filters)
        )

    def combined_join(self, join_method: str, table: Union[Table, 'CombinedTable'], *args, **kwargs) -> 'CombinedTable':
        """Applies the specified join method to the combined table and another table,
        and produces a new CombinedTable.  The join method is specified as a string.
        Using the combined_join method to join a table retains all partitioning information, 
        so queries may be faster and more efficient than the equivalent join method.

        If the table is a CombinedTable, the historical tables are joined together and the live tables are joined together.
        If the table is a Table, the historical table is joined with the other table and the live table is joined with the other table.

        Note that the results are not identical to the join method.
        The results are the same as if the join method was applied to the historical and live tables separately,
        and then the results were combined.

        Args:
            join_method (str): the join method to apply
            table (Union[Table, CombinedTable]): the table to join with
            *args: the positional arguments for the join method
            **kwargs: the keyword arguments for the join method

        Returns:
            a new CombinedTable object
        """

        jm = getattr(type(self.historical), join_method)

        if isinstance(table, type(self.historical)):
            return CombinedTable(
                self._merge,
                jm(self.historical, table, *args, **kwargs),
                jm(self.live, table, *args, **kwargs)
            )
        elif isinstance(table, CombinedTable):
            return CombinedTable(
                self._merge,
                jm(self.historical, table.historical, *args, **kwargs),
                jm(self.live, table.live, *args, **kwargs)
            )
        else:
            raise ValueError(f"Invalid type {type(table)} for argument table")
