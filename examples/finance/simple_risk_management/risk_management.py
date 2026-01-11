# Description: This script demonstrates a real-time risk management system for a portfolio of stocks and options.
#
# Overview:
# - Calculates option Greeks (Delta, Gamma, Theta, Vega, Rho) using Black-Scholes model
# - Aggregates risk metrics across portfolio positions
# - Monitors jump risk (potential P&L from sudden 10% market moves)
# - Provides risk alerts when thresholds are exceeded
# - Includes reactive UI for interactive risk analysis
#
# Features:
# - Real-time Greeks calculation with 5-second updates
# - Beta-adjusted dollar delta for market-correlated risk
# - Hierarchical risk rollup (by symbol, expiration, strike)
# - Table listener for risk threshold alerts
# - Interactive dashboard with account/symbol/expiration filters
#
# Output Tables:
# - securities: Master list of stocks and options
# - price_history: Simulated real-time prices and volatility
# - trade_history: All simulated trades
# - portfolio_history: Position changes over time
# - portfolio_current: Current positions
# - greek_current: Current Greeks and theoretical values
# - betas: Beta coefficients for market correlation
# - risk_all: Detailed risk per position
# - risk_roll: Hierarchical risk aggregation
# - jump: Jump risk by symbol
# - jump_alerts: Symbols exceeding risk limits
# - reactive_risk: Interactive risk UI component

############################################################################################################
# Install the setup package from GitHub
############################################################################################################

import sys
import tempfile
import urllib.request

url = "https://raw.githubusercontent.com/chipkent/deephaven-example-use-cases/main/examples/finance/simple_risk_management/setup_risk_management.py"
tempdir = tempfile.TemporaryDirectory()
sys.path.insert(0, tempdir.name)

with open(f'{tempdir.name}/setup_risk_management.py', 'w') as f:
    with urllib.request.urlopen(url) as response:
        f.write(response.read().decode())

############################################################################################################

from deephaven import time_table, updateby as uby, agg
from deephaven.plot import Figure
from deephaven.table_listener import listen

from setup_risk_management import (simulate_market_data,
                                   black_scholes_price,
                                   black_scholes_delta,
                                   black_scholes_gamma,
                                   black_scholes_theta,
                                   black_scholes_vega,
                                   black_scholes_rho)

rate_risk_free = 0.05

securities, price_history, trade_history, betas = simulate_market_data(rate_risk_free)

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
    .sum_by("USym")

jump_formatted = securities \
    .select_distinct("USym") \
    .sort("USym") \
    .natural_join(jump, "USym")

jump_risk = Figure() \
    .figure_title("Jump Risk") \
    .plot_cat("Down 10%", jump_formatted, "USym", "JumpDown10") \
    .plot_cat("Up 10%", jump_formatted, "USym", "JumpUp10") \
    .show()

############################################################################################################
# Alert on risk
############################################################################################################

def listener_function(update, is_replay):
    over_risk = {**update.added(), **update.modified()}
    under_risk = update.removed()

    if over_risk:
        print(f"EXCESSIVE RISK ... SLACK SOMEONE TO DO SOMETHING: usyms={over_risk['USym']}")

    if under_risk:
        print(f"RISK BACK TO NORMAL: usyms={under_risk['USym']}")


risk_limit = -20000.0
jump_alerts = jump.where("JumpDown10 < risk_limit")
handle = listen(jump_alerts, listener_function, do_replay=True)

# Run handle.stop() to stop the listener
# Run handle.start() to restart the listener

############################################################################################################
# Reactive risk UI
############################################################################################################

from deephaven import ui
from deephaven.time import to_j_instant

#TODO: memoize
risk_all_sort = risk_all.sort(["USym", "Expiry", "Strike"])
risk_all_part = risk_all_sort.partition_by("Account")

jump_account = risk_all.view(["Account", "USym", "JumpUp10", "JumpDown10"]).sum_by(["Account", "USym"])

jump_account_formatted = securities \
    .select_distinct("USym") \
    .sort("USym") \
    .join(jump_account.select_distinct("Account").sort("Account")) \
    .natural_join(jump_account, ["Account", "USym"]) \
    .partition_by("Account")


@ui.component
def risk_view():
    account, set_account = ui.use_state()
    usym, set_usym = ui.use_state()
    expiry, set_expiry = ui.use_state()

    #TODO: add select all
    pick_account = ui.picker(risk_all.select_distinct("Account").sort("Account"), label_column="Account", label="Account", is_selected=account, on_change=set_account)
    pick_usym = ui.text_field(label="USym", value=usym, on_change=set_usym)
    #TODO: add select all
    pick_expiry = ui.picker(risk_all.select_distinct("Expiry").sort("Expiry"), label_column="Expiry", label="Expiry", is_selected=expiry, on_change=set_expiry)

    print(f"DEBUG: {account} {usym} {expiry} {type(expiry)}")

    j_expiry = to_j_instant(int(expiry)) if expiry and expiry != "null" else None
    risk = risk_all_part.get_constituent([account]) if account else risk_all_sort
    risk = risk.where("USym = usym") if usym else risk
    risk = risk.where("Expiry = j_expiry") if expiry else risk

    jump_account_data = jump_account_formatted.get_constituent("Account") if account else jump_formatted

    jump_account_risk = Figure() \
        .figure_title("Jump Risk") \
        .plot_cat("Down 10%", jump_account_data, "USym", "JumpDown10") \
        .plot_cat("Up 10%", jump_account_data, "USym", "JumpUp10") \
        .show()


    # ui.dashboard

    # return [
    #     # ui.row(ui.stack(ui.panel(risk_roll)), ui.stack(ui.panel(jump_risk))),
    #     # ui.row(risk_roll, jump_risk, jump_alerts),
    #     # ui.row(risk_roll, jump_alerts),
    #     # ui.flex(risk_roll, jump_risk, jump_alerts, direction="row"),
    #     # ui.flex(risk_roll, jump_alerts, direction="row"),
    #     # ui.tabs(risk_roll, jump_risk),

    #     risk_roll,

    #     ui.tabs(ui.tab_panels(ui.item(risk_roll), ui.item(risk))),

    #     #TODO: jump_risk
    #     # ui.tabs(jump_risk, betas),

    #     # jump_account_risk,

    # # ui.row(
    # #     pick_account,
    # #     pick_usym,
    # #     pick_expiry,
    # # ),

    # # ui.flex(
    # #     pick_account,
    # #     pick_usym,
    # #     pick_expiry,
    # #     direction="row"
    # # ),

    # ui.column(
    #         ui.row(
    #     pick_account,
    #     pick_usym,
    #     pick_expiry,
    # ),
    # risk,
    # # jump_account_risk,
    # )

    # # risk,
    # ]

    # return ui.column(
    #     risk_roll,
    #     # ui.tabs(ui.tab_panels(ui.item(risk_roll), ui.item(risk))),
    #     # ui.row(pick_account, pick_usym, pick_expiry),
    #     betas.where("USym = usym") if usym else betas,
    #     risk,
    # )

    return ui.flex(
        risk_roll,
        # ui.tabs(ui.tab_panels(ui.item(risk_roll), ui.item(risk))),

        ui.flex(
            ui.flex(pick_account, pick_usym, pick_expiry, direction="row"),
            risk,
            direction="column"
        ),

        # ui.row(pick_account, pick_usym, pick_expiry),
        # betas.where("USym = usym") if usym else betas,
        # risk,
        direction="column"
    )

reactive_risk = risk_view()

# jump_alerts # add control table?
# trades # trades

# betas # betas


