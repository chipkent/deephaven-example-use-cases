
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
