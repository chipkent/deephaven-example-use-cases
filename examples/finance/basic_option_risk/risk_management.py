############################################################################################################
# Basic Option Risk Management Example
#
# This example demonstrates option risk management fundamentals in a single, self-contained script.
# All components (Black-Scholes pricing, market simulation, Greeks calculation, risk aggregation)
# are included inline for easy learning and experimentation.
#
# Key Features:
# - Self-contained: No external dependencies or setup files
# - Educational: See the complete workflow in one place
# - Risk aggregation: Multiple views of portfolio risk (by symbol, expiry, net)
# - Trade analysis: Post-trade P&L analysis with forward-looking price movement
#
# Outputs:
# - securities: Master list of tradeable stocks and options
# - price_history / price_current: Simulated market data
# - greek_history / greek_current: Option Greeks (Delta, Gamma, Theta, Vega, Rho)
# - trade_history: Simulated trades
# - portfolio_history / portfolio_current: Position tracking
# - risk_all: Detailed risk per position
# - risk_ue: Risk aggregated by symbol and expiration
# - risk_u: Risk aggregated by symbol
# - risk_e: Risk aggregated by expiration
# - risk_net: Total portfolio risk
# - trade_pnl: Post-trade price movement analysis
# - trade_pnl_by_sym: P&L aggregated by symbol
#
# Comparison to other option risk examples:
# - simple_risk_management: Interactive UI, multi-account tracking, real-time alerts
# - option_risk: Modular structure with step-by-step components and slippage analysis
#
# This basic version is simpler and self-contained, while simple_risk_management includes:
# - Modular structure with separate setup file
# - Account-based portfolio tracking
# - Interactive UI with reactive filters
# - Risk alert system with table listeners
# - Hierarchical rollup tables for drill-down
#
# Use this basic_option_risk example for:
# - Learning option risk fundamentals
# - Quick prototyping and experimentation
# - Understanding the complete workflow in one place
#
# Use simple_risk_management for:
# - Production-like risk monitoring with interactive UI
# - Multi-account portfolio management
# - Real-time risk alerts
#
# Use option_risk for:
# - Step-by-step learning of each component
# - Understanding modular code organization
# - Advanced features like slippage analysis
############################################################################################################

# from deephaven_server import Server
# _s = Server(port=10000, jvm_args=["-Xmx16g"])
# _s.start()

import math
import numpy as np
import numpy.typing as npt
import numba
from datetime import date, datetime, timedelta
from deephaven import time_table, empty_table, merge, updateby as uby, dtypes as dht

############################################################################################################
# Black-Scholes Option Pricing Model
#
# Industry-standard model for calculating option prices and Greeks (sensitivity measures).
# Uses Numba's @vectorize decorator for JIT compilation and high-performance vectorized operations.
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
# Underlying price simulation
#
# Simulate the price and volatility of a set of underlying securities
############################################################################################################

usyms = ["AAPL", "GOOG", "MSFT", "AMZN", "FB", "TSLA", "NVDA", "INTC", "CSCO", "ADBE", "SPY", "QQQ", "DIA", "IWM", "GLD", "SLV", "USO", "UNG", "TLT", "IEF", "LQD", "HYG", "JNK"]
usyms_array = dht.array(dht.string, usyms)
last_price = {s: round(np.abs(np.random.normal(100, 30.0)), 2) for s in usyms}
last_vol = {s: np.abs(np.random.normal(0.4, 0.2))+0.03 for s in usyms}

def gen_sym() -> str:
    """ Generate a random symbol """
    return usyms[np.random.randint(0, len(usyms))]

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

rate_risk_free = 0.05

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
price_current = price_history.last_by(["USym", "Strike", "Expiry", "Parity"])

_underlying_prices = None
_option_prices = None

############################################################################################################
# Greeks
#
# Calculate the greeks for the securites
############################################################################################################

greek_history = price_history \
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
        "UMidUp10 = UMid * 1.1",
        "UMidDown10 = UMid * 0.9",
        "Up10 = black_scholes_price(UMidUp10, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
        "Down10 = black_scholes_price(UMidDown10, Strike, rate_risk_free, DT, VolMid, IsCall, IsStock)",
        "JumpUp10 = Up10 - Theo",
        "JumpDown10 = Down10 - Theo",
    ]) \
    .drop_columns(["UMidUp10", "UMidDown10", "Up10", "Down10"])

greek_current = greek_history.last_by(["USym", "Strike", "Expiry", "Parity"])


############################################################################################################
# Trade simulation
#
# Simulate a series of trades
############################################################################################################

def get_random_strike(sym: str) -> float:
    """ Get a random strike for a given underlying symbol """
    return np.random.choice(strikes[sym])

trade_history = time_table("PT00:00:01") \
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
# Portfolio
#
# Calculate the current portfolio and history
############################################################################################################

portfolio_history = trade_history \
    .update_by([uby.cum_sum("Position=TradeSize")], ["USym", "Strike", "Expiry", "Parity"])

portfolio_current = portfolio_history \
    .last_by(["USym", "Strike", "Expiry", "Parity"]) \
    .view(["USym", "Strike", "Expiry", "Parity", "Position"])


############################################################################################################
# Risk
#
# Calculate the risk for the portfolio in different ways
############################################################################################################

betas = empty_table(1) \
    .update(["USym = usyms_array"]) \
    .ungroup() \
    .update(["Beta = random() * 2 - 0.5"])

risk_all = greek_current \
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
        "Theo", "DollarDelta", "BetaDollarDelta", "GammaPercent", "VegaPercent", "Theta", "Rho", "JumpUp10", "JumpDown10"])

risk_ue = risk_all \
    .drop_columns(["Strike", "Parity"]) \
    .sum_by(["USym", "Expiry"])

risk_u = risk_ue \
    .drop_columns("Expiry") \
    .sum_by("USym")

risk_e = risk_ue \
    .drop_columns("USym") \
    .sum_by("Expiry")

risk_net = risk_ue \
    .drop_columns(["USym", "Expiry"]) \
    .sum_by()

############################################################################################################
# Trade analysis
#
# Calculate the PnL for the trades with a 10 minute holding period
############################################################################################################

trade_pnl = trade_history \
    .view(["Timestamp", "USym", "Strike", "Expiry", "Parity", "TradeSize", "TradePrice"]) \
    .aj(price_history.update("Timestamp=Timestamp-'PT10m'"), ["USym", "Strike", "Expiry", "Parity", "Timestamp"], ["FutureBid=Bid", "FutureAsk=Ask"]) \
    .update([
        "FutureMid = (FutureBid + FutureAsk) / 2",
        "PriceChange = FutureMid - TradePrice",
        "PnL = TradeSize * PriceChange",
    ])

trade_pnl_by_sym = trade_pnl \
    .view(["USym", "PnL"]) \
    .sum_by("USym")

