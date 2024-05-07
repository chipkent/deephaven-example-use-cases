
""" A package that provides a CombinedTable class for pydeephaven clients
that combines a historical and live table into a single table.

See: https://deephaven.io/enterprise/docs/coreplus/coreplus-python-client/
"""

from uuid import uuid4

from combined_table_common import CombinedTable
from pydeephaven import Session, Table


def _db_table(session: Session, namespace: str, table_name: str, is_live: bool) -> Table:
    """ Get a table from the database on the server.

    Args:
        session: The session to use.
        namespace: The namespace of the table.
        table_name: The name of the table.
        is_live: True for live table, False for historical table.
    """
    tid = uuid4().int
    session.run_script(
        f"""_temp_{tid} = db.{"live_table" if is_live else "historical_table"}("{namespace}", "{table_name}")"""
    )
    t = session.open_table(f"_temp_{tid}")
    session.run_script(f"""del _temp_{tid}""")
    return t


def combined_table(session: Session, namespace: str, table_name: str) -> CombinedTable:
    """ Create a combined table for the given namespace and table name.

    The live table is for today according to `today()`.
    The historical table is for all dates less than today.

    Args:
        session: The session to use.
        namespace: The namespace of the table.
        table_name: The name of the table.

    Returns:
        A CombinedTable object.
    """
    hist = _db_table(session, namespace, table_name, is_live=False)
    live = _db_table(session, namespace, table_name, is_live=True)
    return CombinedTable(session.merge_tables, hist, live, hist_filters=["Date < today()"], live_filters=["Date = today()"])
