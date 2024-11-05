
""" Simulate a security master table with underlying and option securities """

from datetime import date, datetime, timedelta
import numpy as np
import numpy.typing as npt
import jpy
from deephaven import empty_table, merge, dtypes as dht
from deephaven.table import Table


def simulate_strikes(underlyings: dict[str, float], sym: str) -> npt.NDArray[np.float64]:
    """ Simulate a range of strikes """
    open = underlyings[sym]
    ref = round(open, 0)
    start = ref - 5
    stop = ref + 5
    return np.arange(start, stop, step=1)


def simulate_expiries() -> jpy.JType:
    """ Simulate a range of expiries """
    return dht.array(dht.Instant, [
        datetime.combine(date.today() + timedelta(days=30), datetime.min.time()),
        datetime.combine(date.today() + timedelta(days=60), datetime.min.time()),
    ])


def simulate_security_master(underlyings: dict[str, float]) -> Table:
    """ Simulate  a security master table with underlying and option securities """

    usyms_array = dht.array(dht.string, list(underlyings.keys()))

    underlying_securities = empty_table(1) \
        .update(["Type=`STOCK`", "USym = usyms_array"]) \
        .ungroup() \
        .update([
            "Strike = NULL_DOUBLE",
            "Expiry = (Instant) null",
            "Parity = (String) null",
        ])

    def compute_strikes(sym: str) -> npt.NDArray[np.float64]:
        return simulate_strikes(underlyings, sym)

    # expiry_array = simulate_expiries()
    expiry_array = dht.array(dht.Instant, simulate_expiries())

    option_securities = empty_table(1) \
        .update(["Type=`OPTION`", "USym = usyms_array"]) \
        .ungroup() \
        .update(["Strike = compute_strikes(USym)"]) \
        .ungroup() \
        .update(["Expiry = expiry_array"]) \
        .ungroup() \
        .update(["Parity = new String[] {`CALL`, `PUT`}"]) \
        .ungroup() \
        .view(["Type", "USym", "Strike", "Expiry", "Parity"])

    return merge([underlying_securities, option_securities]) \
        .sort(["USym", "Type", "Expiry", "Strike", "Parity"])

