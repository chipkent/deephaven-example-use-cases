
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