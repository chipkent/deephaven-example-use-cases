""" Setup the risk management example. """

import math
import numpy as np
import numpy.typing as npt
import numba
from datetime import date, datetime, timedelta
from deephaven import time_table, empty_table, merge, dtypes as dht
from deephaven.table import Table

############################################################################################################
# Black-Scholes
#
# Write a Black-Scholes option pricing model in Python using Numba for vectorization.
############################################################################################################

@numba.vectorize(['float64(float64)'])
def norm_cdf(x):
    """ Cumulative distribution function for the standard normal distribution """
    return (1.0 + math.erf(x / math.sqrt(2.0))) / 2.0


@numba.vectorize(['float64(float64)'])
def norm_pdf(x):
    """ Probability density function for the standard normal distribution """
    return math.exp(-x**2 / 2.0) / math.sqrt(2.0 * math.pi)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_price(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option price for a call/put """

    if is_stock:
        return s

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)

    if is_call:
        return s * norm_cdf(d1) - k * np.exp(-r * t) * norm_cdf(d2)
    else:
        return k * np.exp(-r * t) * norm_cdf(-d2) - s * norm_cdf(-d1)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_delta(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option delta for a call/put """

    if is_stock:
        return 1.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    # d2 = d1 - vol * np.sqrt(T)

    if is_call:
        return norm_cdf(d1)
    else:
        return -norm_cdf(-d1)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean)'])
def black_scholes_gamma(s, k, r, t, vol, is_stock):
    """ Calculates the Black-Scholes option gamma """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    # d2 = d1 - vol * np.sqrt(T)

    return norm_pdf(d1) / (s * vol * np.sqrt(t))


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_theta(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option theta """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)

    if is_call:
        return - ((s * norm_pdf(d1) * vol) / (2 * np.sqrt(t))) - r * k * np.exp(-r * t) * norm_cdf(d2)
    else:
        return - ((s * norm_pdf(d1) * vol) / (2 * np.sqrt(t))) + r * k * np.exp(-r * t) * norm_cdf(-d2)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean)'])
def black_scholes_vega(s, k, r, t, vol, is_stock):
    """ Calculates the Black-Scholes option vega """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    return s * np.sqrt(t) * norm_pdf(d1)


@numba.vectorize(['float64(float64, float64, float64, float64, float64, boolean, boolean)'])
def black_scholes_rho(s, k, r, t, vol, is_call, is_stock):
    """ Calculates the Black-Scholes option rho """

    if is_stock:
        return 0.0

    d1 = (np.log(s / k) + (r + vol ** 2 / 2) * t) / (vol * np.sqrt(t))
    d2 = d1 - vol * np.sqrt(t)

    if is_call:
        return 0.01 * k * t * np.exp(-r * t) * norm_cdf(d2)
    else:
        return 0.01 * -k * t * np.exp(-r * t) * norm_cdf(-d2)


############################################################################################################
# Simulate market and trading data
############################################################################################################

# noinspection PyUnusedLocal
def simulate_market_data(usyms: list[str], risk_free_rate: float) -> tuple[Table, Table, Table, Table]:
    """ Simulate market data for a set of underlying securities and options.

    Args:
        usyms: List of underlying symbols
        risk_free_rate: The risk-free rate

    Returns:
        Tuple of tables containing the simulated securities, price history, trade history, betas
    """

    ############################################################################################################
    # Underlying price simulation
    #
    # Simulate the price and volatility of a set of underlying securities
    ############################################################################################################

    # noinspection PyUnusedLocal
    usyms_array = dht.array(dht.string, usyms)
    last_price = {s: round(np.abs(np.random.normal(100, 30.0)), 2) for s in usyms}
    last_vol = {s: np.abs(np.random.normal(0.4, 0.2)) + 0.03 for s in usyms}

    # noinspection PyUnusedLocal
    def gen_sym() -> str:
        """ Generate a random symbol """
        return usyms[np.random.randint(0, len(usyms))]

    # noinspection PyUnusedLocal
    def gen_price(sym: str) -> float:
        """ Generate a random price for a given symbol """
        p = last_price[sym]
        p += (np.random.random() - 0.5)
        p = abs(p)
        last_price[sym] = p
        return round(p, 2)

    # noinspection PyUnusedLocal
    def gen_vol(sym: str) -> float:
        """ Generate a random volatility for a given symbol """
        v = last_vol[sym]
        v += (np.random.random() - 0.5) * 0.01
        v = abs(v)
        last_vol[sym] = v
        return v

    _underlying_securities = empty_table(1) \
        .update(["Type=`STOCK`", "USym = usyms_array"]) \
        .ungroup() \
        .update([
            "Strike = NULL_DOUBLE",
            "Expiry = (Instant) null",
            "Parity = (String) null",
        ])

    _underlying_prices = time_table("PT00:00:00.1") \
        .update([
            "Type = `STOCK`",
            "USym = gen_sym()",
            "Strike = NULL_DOUBLE",
            "Expiry = (Instant) null",
            "Parity = (String) null",
            "UBid = gen_price(USym)",
            "UAsk = UBid + randomInt(1, 10)*0.01",
            "VolBid = gen_vol(USym)",
            "VolAsk = VolBid + randomInt(1, 10)*0.01",
            "Bid = UBid",
            "Ask = UAsk",
        ])

    ############################################################################################################
    # Option price simulation
    #
    # Simulate the price of a set of options based on the underlying securities
    ############################################################################################################

    def compute_strikes(spot: float) -> npt.NDArray[np.float64]:
        """ Compute the option strikes from a given underlying opening price """
        ref = round(spot, 0)
        start = ref - 5
        stop = ref + 5
        return np.arange(start, stop, step=1)

    strikes = {s: compute_strikes(p) for s, p in last_price.items()}

    # noinspection PyUnusedLocal
    def get_strikes(sym: str) -> npt.NDArray[np.float64]:
        """ Get the strikes for a given symbol """
        return strikes[sym]

    # noinspection PyUnusedLocal
    expiry_array = dht.array(dht.Instant, [
        datetime.combine(date.today() + timedelta(days=30), datetime.min.time()),
        datetime.combine(date.today() + timedelta(days=60), datetime.min.time()),
    ])

    _option_securities = empty_table(1) \
        .update(["Type=`OPTION`", "USym = usyms_array"]) \
        .ungroup() \
        .update(["Strike = get_strikes(USym)"]) \
        .ungroup() \
        .update(["Expiry = expiry_array"]) \
        .ungroup() \
        .update(["Parity = new String[] {`CALL`, `PUT`}"]) \
        .ungroup() \
        .view(["Type", "USym", "Strike", "Expiry", "Parity"])

    _option_prices = _underlying_prices \
        .view(["Timestamp", "USym", "UBid", "UAsk", "VolBid", "VolAsk"]) \
        .join(_option_securities, "USym") \
        .view(["Timestamp", "Type", "USym", "Strike", "Expiry", "Parity", "UBid", "UAsk", "VolBid", "VolAsk"]) \
        .update([
            "DT = diffYearsAvg(Timestamp, Expiry)",
            "IsStock = Type == `STOCK`",
            "IsCall = Parity == `CALL`",
            "Bid = black_scholes_price(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
            "Ask = black_scholes_price(UAsk, Strike, rate_risk_free, DT, VolAsk, IsCall, IsStock)",
        ]) \
        .drop_columns(["DT", "IsStock", "IsCall"])

    ############################################################################################################
    # Securities
    #
    # Combine the underlying and option securities into a single table
    ############################################################################################################

    securities = merge([_underlying_securities, _option_securities])

    _underlying_securities = None
    _option_securities = None

    ############################################################################################################
    # Prices
    #
    # Combine the underlying and option prices into a single table
    ############################################################################################################

    price_history = merge([_underlying_prices, _option_prices])

    ############################################################################################################
    # Trade simulation
    #
    # Simulate a series of trades
    ############################################################################################################

    # noinspection PyUnusedLocal
    def get_random_strike(sym: str) -> float:
        """ Get a random strike for a given underlying symbol """
        return np.random.choice(strikes[sym])

    trade_history = time_table("PT00:00:01") \
        .update([
            "Type = random() < 0.3 ? `STOCK` : `OPTION`",
            "USym = usyms_array[randomInt(0, usyms_array.length)]",
            "Strike = Type == `STOCK` ? NULL_DOUBLE : get_random_strike(USym)",
            "Expiry = Type == `STOCK` ? null : _expiry_array[randomInt(0, _expiry_array.length)]",
            "Parity = Type == `STOCK` ? null : random() < 0.5 ? `CALL` : `PUT`",
            "TradeSize = randomInt(-1000, 1000)",
        ]) \
        .aj(price_history, ["USym", "Strike", "Expiry", "Parity", "Timestamp"], ["Bid", "Ask"]) \
        .update(["TradePrice = random() < 0.5 ? Bid : Ask"])

    ############################################################################################################
    # Risk betas
    ############################################################################################################

    betas = empty_table(1) \
        .update(["USym = usyms_array"]) \
        .ungroup() \
        .update(["Beta = random() * 2 - 0.5"])

    return securities, price_history, trade_history, betas

