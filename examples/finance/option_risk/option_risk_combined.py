
############################################################################################################
# Option Model
############################################################################################################

""" Black-Scholes option pricing model in Python using Numba for vectorization """

import math
import numba
import numpy as np


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
# Security Master
############################################################################################################


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


############################################################################################################
# Betas
############################################################################################################


""" Simulate beta values for a set of underlyings. """

from deephaven import new_table
from deephaven.table import Table
from deephaven.column import string_col

def simulate_betas(underlyings: dict[str, float]) -> Table:
    """ Simulate beta values for a set of underlyings. """

    return new_table([string_col("USym", list(underlyings.keys()))]) \
        .update(["Beta = random() * 2 - 0.5"])


############################################################################################################
# Market Prices
############################################################################################################


""" Simulate market prices for a given set of underlyings and a security master table """

import numpy as np

from deephaven import empty_table, time_table, merge, dtypes as dht
from deephaven.table import Table

def simulate_market_prices(underlyings: dict[str, float], sec_master: Table, update_interval: str, rate_risk_free: float) -> Table:
    """ Simulate market prices for a given set of underlyings and a security master table """

    usyms_array = dht.array(dht.string, list(underlyings.keys()))
    last_price = {usym: round(np.abs(np.random.normal(open, 30.0)), 2) for usym, open in underlyings.items()}
    last_vol = {usym: np.abs(np.random.normal(0.4, 0.2)) + 0.03 for usym in underlyings.keys()}

    def gen_sym() -> str:
        """ Generate a random symbol """
        return np.random.choice(usyms_array)

    def gen_price(sym: str) -> float:
        """ Generate a random price for a given symbol """
        p = last_price[sym]
        p += (np.random.random() - 0.5)
        p = abs(p)
        last_price[sym] = p
        return round(p, 2)

    def gen_vol(sym: str) -> float:
        """ Generate a random volatility for a given symbol """
        v = last_vol[sym]
        v += (np.random.random() - 0.5) * 0.01
        v = abs(v)
        last_vol[sym] = v
        return v

    underlying_prices = time_table(update_interval) \
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

    option_securities = sec_master.where("Type = `OPTION`")

    option_prices = underlying_prices \
        .view(["Timestamp", "USym", "UBid", "UAsk", "VolBid", "VolAsk"]) \
        .join(option_securities, "USym") \
        .view(["Timestamp", "Type", "USym", "Strike", "Expiry", "Parity", "UBid", "UAsk", "VolBid", "VolAsk"]) \
        .update([
            "DT = diffYearsAvg(Timestamp, Expiry)",
            "IsStock = Type == `STOCK`",
            "IsCall = Parity == `CALL`",
            "Bid = black_scholes_price(UBid, Strike, rate_risk_free, DT, VolBid, IsCall, IsStock)",
            "Ask = black_scholes_price(UAsk, Strike, rate_risk_free, DT, VolAsk, IsCall, IsStock)",
        ]) \
        .drop_columns(["DT", "IsStock", "IsCall"])

    return merge([underlying_prices, option_prices]).sort("Timestamp")


############################################################################################################
# Trades
############################################################################################################


""" Simulate trades for a given set of underlyings """

import numpy as np
import numpy.typing as npt

from deephaven import time_table
from deephaven.table import Table

def simulate_trades(underlyings: dict[str, float], price_history: Table, update_interval: str) -> Table:
    """ Simulate trades for a given set of underlyings """

    usyms_array = dht.array(dht.string, list(underlyings.keys()))
    strikes = {sym: simulate_strikes(underlyings, sym) for sym in underlyings.keys()}

    def get_random_strike(sym: str) -> float:
        """ Get a random strike for a given underlying symbol """
        return np.random.choice(strikes[sym])

    expiry_array = simulate_expiries()

    return time_table(update_interval) \
        .update([
            "Type = random() < 0.3 ? `STOCK` : `OPTION`",
            "USym = usyms_array[randomInt(0, usyms_array.length)]",
            "Strike = Type == `STOCK` ? NULL_DOUBLE : get_random_strike(USym)",
            "Expiry = Type == `STOCK` ? null : expiry_array[randomInt(0, expiry_array.length)]",
            "Parity = Type == `STOCK` ? null : random() < 0.5 ? `CALL` : `PUT`",
            "TradeSize = randomInt(-1000, 1000)",
        ]) \
        .aj(price_history, ["USym", "Strike", "Expiry", "Parity", "Timestamp"], ["Bid", "Ask"]) \
        .update(["TradePrice = random() < 0.5 ? Bid : Ask"])


############################################################################################################
# Greeks
############################################################################################################


""" Compute the greeks for the securities """

from deephaven import time_table
from deephaven.table import Table

def compute_greeks(current_prices: Table, update_interval: str, rate_risk_free: float) -> Table:
    """ Compute the greeks for the securities """

    return current_prices \
        .snapshot_when(time_table(update_interval).drop_columns("Timestamp")) \
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
            "UMidUp10 = UMid * 1.1",
            "UMidDown10 = UMid * 0.9",
            "Up10 = black_scholes_price(UMidUp10, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
            "Down10 = black_scholes_price(UMidDown10, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
            "JumpUp10 = Up10 - Theo",
            "JumpDown10 = Down10 - Theo",
        ])


############################################################################################################
# Risk
############################################################################################################


""" Compute risk for a portfolio of stock and options. """

from deephaven.table import Table
def compute_risk(greeks_current: Table, portfolio_current: Table, betas: Table) -> Table:
    """ Compute risk for a portfolio of stock and options. """

    return greeks_current \
        .natural_join(portfolio_current, ["USym", "Strike", "Expiry", "Parity"]) \
        .natural_join(betas, "USym") \
        .update([
            "Theo = Theo * Position",
            "DollarDelta = UMid * Delta * Position",
            "BetaDollarDelta = Beta * DollarDelta",
            "GammaPercent = UMid * Gamma * Position",
            "Theta = Theta * Position",
            "VegaPercent = VolMid * Vega * Position",
            "Rho = Rho * Position",
            "JumpUp10 = JumpUp10 * Position",
            "JumpDown10 = JumpDown10 * Position",
        ]) \
        .view([
            "USym", "Strike", "Expiry", "Parity",
            "Theo", "DollarDelta", "BetaDollarDelta", "GammaPercent", "VegaPercent", "Theta", "Rho", "JumpUp10", "JumpDown10"
        ])


############################################################################################################
# Risk Rollup
############################################################################################################


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
            include_constituents=True,
        )


############################################################################################################
# Slippage
############################################################################################################



""" Compute the slippage for the given trade. Slippage is defined as price movement in a short period after the trade. """

from deephaven.table import Table, RollupTable
from deephaven import agg

def compute_slippage(trade_history: Table, price_history: Table, holding_period: str) -> RollupTable:
    """ Compute the slippage for the given trade. Slippage is defined as price movement in a short period after the trade. """

    trade_pnl = trade_history \
        .view(["Timestamp", "USym", "Strike", "Expiry", "Parity", "TradeSize", "TradePrice"]) \
        .aj(price_history.update(f"Timestamp=Timestamp-'{holding_period}'"), ["USym", "Strike", "Expiry", "Parity", "Timestamp"],
            ["FutureBid=Bid", "FutureAsk=Ask"]) \
        .update([
            "FutureMid = (FutureBid + FutureAsk) / 2",
            "PriceChange = FutureMid - TradePrice",
            "PnL = TradeSize * PriceChange",
        ])

    by = ["USym", "Expiry", "Strike", "Parity"]

    return trade_pnl \
        .rollup(
            aggs=[agg.last("Timestamp"), agg.sum_(["PnL", "TradeSize"]), agg.avg("AvgTradePrice=TradePrice")],
            by=by,
            include_constituents=True,
        )


############################################################################################################
# Main
############################################################################################################


""" This script simulates a trading environment and computes the risk for a portfolio of options and stocks. """

from deephaven import updateby as uby

rate_risk_free = 0.05

############################################################################################################
# Simulate inputs
############################################################################################################

# USyms and opening prices to simulate
underlyings = {
    "AAPL": 223.61,
    "GOOG": 171.93,
    "MSFT": 414.12,
    "AMZN": 199.68,
    "META": 572.00,
    "TSLA": 254.54,
    "NVDA": 139.71,
    "INTC": 23.40,
    "CSCO": 56.13,
    "ADBE": 485.84,
    "SPY": 576.25,
    "QQQ": 492.62,
    "DIA": 422.50,
    "IWM": 222.40,
    "GLD": 253.24,
    "SLV": 29.76,
    "USO": 74.49,
    "UNG": 12.69,
    "TLT": 91.80,
    "IEF": 93.86,
    "LQD": 108.71,
    "HYG": 79.00,
    "JNK": 96.07,
}

# Simulate the security master table
sec_master = simulate_security_master(underlyings)

# Simulate the betas
betas = simulate_betas(underlyings)

# Simulate the market prices
prices_history = simulate_market_prices(underlyings, sec_master, update_interval="PT00:00:00.1", rate_risk_free=rate_risk_free)

# Simulate the trades
trade_history = simulate_trades(underlyings, prices_history, update_interval="PT00:00:01")

############################################################################################################
# Portfolio analysis
############################################################################################################

# Compute the current prices
prices_current = prices_history.last_by(["USym", "Strike", "Expiry", "Parity"])

# Compute the portfolio history
portfolio_history = trade_history \
    .update_by([uby.cum_sum("Position=TradeSize")], ["USym", "Strike", "Expiry", "Parity"])

# Compute the current portfolio
portfolio_current = portfolio_history \
    .last_by(["USym", "Strike", "Expiry", "Parity"]) \
    .view(["USym", "Strike", "Expiry", "Parity", "Position"])

############################################################################################################
# Risk analysis
############################################################################################################

# Compute the greeks
greeks_current = compute_greeks(prices_current, update_interval="PT00:00:05", rate_risk_free=rate_risk_free)

# Compute the risk for the portfolio
risk_all = compute_risk(greeks_current, portfolio_current, betas)

# Compute the risk rollup
risk_rollup = compute_risk_rollup(risk_all)

############################################################################################################
# Slippage analysis
############################################################################################################

# Compute the slippage
slippage = compute_slippage(trade_history, prices_history, holding_period="PT1m")
