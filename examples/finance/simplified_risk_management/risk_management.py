
from deephaven import time_table, updateby as uby, agg
from deephaven.plot import Figure

from setup_risk_management import (simulate_market_data,
                                   black_scholes_price,
                                   black_scholes_delta,
                                   black_scholes_gamma,
                                   black_scholes_theta,
                                   black_scholes_vega,
                                   black_scholes_rho)

usyms = ["AAPL", "GOOG", "MSFT", "AMZN", "FB", "TSLA", "NVDA", "INTC", "CSCO", "ADBE", "SPY", "QQQ", "DIA", "IWM",
         "GLD", "SLV", "USO", "UNG", "TLT", "IEF", "LQD", "HYG", "JNK"]
rate_risk_free = 0.05

securities, price_history, trade_history, betas = simulate_market_data(usyms, rate_risk_free)

############################################################################################################
# Current security prices
############################################################################################################

price_current = price_history.last_by(["USym", "Strike", "Expiry", "Parity"])


############################################################################################################
# Portfolio
#
# Calculate the current portfolio and history
############################################################################################################

portfolio_history = trade_history \
    .update_by([uby.cum_sum("Position=TradeSize")], ["Account", "Type", "USym", "Strike", "Expiry", "Parity"])

portfolio_current = portfolio_history \
    .last_by(["Account", "Type", "USym", "Strike", "Expiry", "Parity"]) \
    .view(["Account", "Type", "USym", "Strike", "Expiry", "Parity", "Position"])


############################################################################################################
# Greeks
#
# Calculate the greeks for the securites
############################################################################################################

greek_current = price_current \
    .snapshot_when(time_table("PT00:00:05").drop_columns("Timestamp")) \
    .update([
        "UMid = (UBid + UAsk) / 2",
        "VolMid = (VolBid + VolAsk) / 2",
        "DT = diffYearsAvg(Timestamp, Expiry)",
        "Rf = (double) rate_risk_free",
        "IsStock = Type == `STOCK`",
        "IsCall = Parity == `CALL`",
        "Theo = black_scholes_price(UMid, Strike, Rf, DT, VolMid, IsCall, IsStock)",
        "Delta = black_scholes_delta(UBid, Strike, Rf, DT, VolBid, IsCall, IsStock)",
        "Gamma = black_scholes_gamma(UBid, Strike, Rf, DT, VolBid, IsStock)",
        "Theta = black_scholes_theta(UBid, Strike, Rf, DT, VolBid, IsCall, IsStock)",
        "Vega = black_scholes_vega(UBid, Strike, Rf, DT, VolBid, IsStock)",
        "Rho = black_scholes_rho(UBid, Strike, Rf, DT, VolBid, IsCall, IsStock)",
        "UMidUp10 = UMid * 1.1",
        "UMidDown10 = UMid * 0.9",
        "Up10 = black_scholes_price(UMidUp10, Strike, Rf, DT, VolMid, IsCall, IsStock)",
        "Down10 = black_scholes_price(UMidDown10, Strike, Rf, DT, VolMid, IsCall, IsStock)",
        "JumpUp10 = Up10 - Theo",
        "JumpDown10 = Down10 - Theo",
    ]) \
    .drop_columns(["UMidUp10", "UMidDown10", "Up10", "Down10"])


############################################################################################################
# Risk
#
# Calculate the risk for the portfolio in different ways
############################################################################################################

risk_all = portfolio_current \
    .natural_join(greek_current, ["Type", "USym", "Strike", "Expiry", "Parity"]) \
    .natural_join(betas, "USym") \
    .view([
        "Account",
        "USym",
        "Strike",
        "Expiry",
        "Parity",
        "Theo = Theo * Position",
        "DollarDelta = UMid * Delta * Position",
        "BetaDollarDelta = Beta * DollarDelta",
        "GammaPercent = UMid * Gamma * Position",
        "Theta = Theta * Position",
        "VegaPercent = VolMid * Vega * Position",
        "Rho = Rho * Position",
        "JumpUp10 = JumpUp10 * Position",
        "JumpDown10 = JumpDown10 * Position",
    ])

risk_roll = risk_all.rollup(
    aggs=[agg.sum_(["Theo", "DollarDelta", "BetaDollarDelta", "GammaPercent", "VegaPercent", "Theta", "Rho", "JumpUp10", "JumpDown10"])],
    by=["Account", "USym", "Expiry", "Strike"],
    include_constituents=False,
)


############################################################################################################
# Plot risk
############################################################################################################

jump = risk_all.view(["USym", "JumpUp10", "JumpDown10"]) \
    .sum_by("USym") \
    .sort("USym")

jump_risk = Figure() \
    .figure_title("Jump Risk") \
    .plot_cat("Down 10%", jump, "USym", "JumpDown10") \
    .plot_cat("Up 10%", jump, "USym", "JumpUp10") \
    .show()


############################################################################################################
# Trade analysis
#
# Calculate the PnL for the trades with a 10 minute holding period
############################################################################################################

trade_pnl = trade_history \
    .view(["Timestamp", "USym", "Strike", "Expiry", "Parity", "TradeSize", "TradePrice"]) \
    .aj(price_history.update("Timestamp=Timestamp-'PT10m'"),
        ["USym", "Strike", "Expiry", "Parity", "Timestamp"],
        ["FutureBid=Bid", "FutureAsk=Ask"]) \
    .update([
        "FutureMid = (FutureBid + FutureAsk) / 2",
        "PriceChange = FutureMid - TradePrice",
        "PnL = TradeSize * PriceChange",
    ])

trade_pnl_by_sym = trade_pnl \
    .view(["USym", "PnL"]) \
    .sum_by("USym")
