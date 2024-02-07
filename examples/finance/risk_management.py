
# from deephaven_server import Server
# _s = Server(port=10000, jvm_args=["-Xmx16g"])
# _s.start()

#TODO: document


#TODO: TARGET
# If you can get me the fake-data.... and ideally orders, trades, positions, pnl (essentially how each turns into the other).....
# Either with fake stock price data or real FeedOS market stock data....
# Then I can do the rest (aggregations, dashboards, plaots, deephaven.ui -- maybe even  pivots [dunno, we'll see if charles' toy is up to a demo task]).
# Extra credit:
# Something for PNL attribution -- some sort of factor thing.
# VAR or some other slide-run risk -- maybe correlation infrastructure -- again can all be fake or statically canned from wherever.
# Really stretch -- though totally unnecessary -- something for a basic options use case.

############################################################################################################
# Black-Scholes
############################################################################################################

import math
import numpy as np
import numba


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
# Underlying price simulation
############################################################################################################

from deephaven import time_table, empty_table

syms = ["AAPL", "GOOG", "MSFT", "AMZN", "FB", "TSLA", "NVDA", "INTC", "CSCO", "ADBE", "SPY", "QQQ", "DIA", "IWM", "GLD", "SLV", "USO", "UNG", "TLT", "IEF", "LQD", "HYG", "JNK"]
last_price = {s: round(np.abs(np.random.normal(100, 30.0)), 2) for s in syms}
last_vol = {s: np.abs(np.random.normal(0.4, 0.2))+0.03 for s in syms}

def gen_sym() -> str:
    """ Generate a random symbol """
    return syms[np.random.randint(0, len(syms))]

def gen_price(sym: str) -> float:
    """ Generate a random price for a given symbol """
    p = last_price[sym]
    p += (np.random.random()-0.5)
    p = abs(p)
    last_price[sym] = p
    return round(p,2)

def gen_vol(sym: str) -> float:
    """ Generate a random volatility for a given symbol """
    v = last_vol[sym]
    v += (np.random.random()-0.5)*0.01
    v = abs(v)
    last_vol[sym] = v
    return v

underlying_prices = time_table("PT00:00:00.1") \
    .update([
        "Type = `STOCK`",
        "USym = gen_sym()",
        "Sym = USym",
        "Strike = NULL_DOUBLE",
        "Expiry = (Instant) null",
        "Parity = (String) null",
        "UBid = gen_price(Sym)",
        "UAsk = UBid + randomInt(1, 10)*0.01",
        "VolBid = gen_vol(Sym)",
        "VolAsk = VolBid + randomInt(1, 10)*0.01",
        "Bid = UBid",
        "Ask = UAsk",
    ])


############################################################################################################
# Underlying price simulation
############################################################################################################

import numpy.typing as npt
from deephaven import dtypes as dht
from datetime import date
from datetime import datetime, timedelta

def compute_strikes(open: float) -> npt.NDArray[np.float64]:
    """ Compute the option strikes from a given underlying opening price """
    ref = round(open,0)
    start = ref - 5
    stop = ref + 5
    return np.arange(start, stop, step=1)

strikes = {s: compute_strikes(p) for s, p in last_price.items() }

def get_strikes(sym: str) -> npt.NDArray[np.float64]:
    """ Get the strikes for a given symbol """
    return strikes[sym]

usyms_array = dht.array(dht.string, syms)

expiry_array = dht.array(dht.Instant, [
    datetime.combine(date.today() + timedelta(days=30), datetime.min.time()),
    datetime.combine(date.today() + timedelta(days=60), datetime.min.time()),
])

option_securities = empty_table(1) \
    .update(["Type=`OPTION`", "USym = usyms_array"]) \
    .ungroup() \
    .update(["Strike = get_strikes(USym)"]) \
    .ungroup() \
    .update(["Expiry = expiry_array"]) \
    .ungroup() \
    .update(["Parity = new String[] {`CALL`, `PUT`}"]) \
    .ungroup() \
    .update("Sym = USym + `/` + toLocalDate(Expiry, 'ET') + `/` + Strike + `/` + Parity") \
    .view(["Type", "USym", "Sym", "Strike", "Expiry", "Parity"])

rate_risk_free = 0.05

option_prices = underlying_prices \
    .view(["Timestamp", "USym", "UBid", "UAsk", "VolBid", "VolAsk"]) \
    .join(option_securities, "USym") \
    .view(["Timestamp", "Type", "USym", "Sym", "Strike", "Expiry", "Parity", "UBid", "UAsk", "VolBid", "VolAsk"]) \
    .update([
        "DT = diffYearsAvg(Timestamp, Expiry)",
        "IsStock = Type == `STOCK`",
        "IsCall = Parity == `CALL`",
        "Bid = black_scholes_price(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
        "Ask = black_scholes_price(UAsk, Strike, rate_risk_free, DT, VolAsk, IsCall, IsStock)",
    ]) \
    .drop_columns(["DT", "IsStock", "IsCall"])


############################################################################################################
# Prices
############################################################################################################

from deephaven import merge

prices = merge([underlying_prices, option_prices])
current_prices = prices.last_by("Sym")

############################################################################################################
# Greeks
############################################################################################################

greeks = prices \
    .snapshot_when(time_table("PT00:00:05").drop_columns("Timestamp")) \
    .update([
        "UMid = (UBid + UAsk) / 2",
        "VolMid = (VolBid + VolAsk) / 2",
        "DT = diffYearsAvg(Timestamp, Expiry)",
        "IsStock = Type == `STOCK`",
        "IsCall = Parity == `CALL`",
        "Theo = black_scholes_price(UMid, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
        "Delta = black_scholes_delta(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
        "Gamma = black_scholes_gamma(UBid, Strike, rate_risk_free, DT, VolBid, IsStock)",
        "Theta = black_scholes_theta(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
        "Vega = black_scholes_vega(UBid, Strike, rate_risk_free, DT, VolBid, IsStock)",
        "Rho = black_scholes_rho(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
    ])

current_greeks = greeks.last_by("Sym")



# from deephaven import time_table, updateby as uby
#
# # Set up a price feed
#
# last_price = {"A" : 100.0, "B": 200.0}
#
# def price_generator(sym: str) -> float:
#     p = last_price[sym]
#     p += (random.random()-0.5)
#     # p = lognormvariate(p, 0.0)
#     last_price["sym"] = p
#     return p
#
# prices = time_table("PT00:00:00.1") \
#     .update([
#         "Sym = ii%2==0 ? `A` : `B`",
#         "Price = price_generator(Sym)",
#     ])
#
# # Set up a trade feed
#
# trades = time_table("PT00:00:01") \
#     .update([
#         "Sym = ii%2==0 ? `A` : `B`",
#         "Size = randomInt(-1000, 1000)",
#         "Direction = Size > 0 ? `LONG` : `SHORT`",
#         ]) \
#     .aj(prices, ["Sym", "Timestamp"], ["Price"])
#
# # Compute portfolio views -- these could also be feeds
#
# portfolio_hist = trades.update_by([uby.cum_sum("Size")], "Sym")
#
# portfolio = portfolio_hist.drop_columns("Direction").last_by("Sym")
#
# # Do some trade analysis
#
# analysis = trades \
#     .aj(
#         prices.update_view(["FutureTimestamp=Timestamp", "Timestamp=Timestamp-'PT00:01:00'"]),
#         ["Sym", "Timestamp"], ["FuturePrice=Price"] \
#         ) \
#     .update_view("PriceChange = FuturePrice-Price")
