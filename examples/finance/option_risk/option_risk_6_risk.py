""" Compute portfolio-level risk metrics by aggregating Greeks with positions.

This module combines:
- Option Greeks (sensitivities from Black-Scholes)
- Portfolio positions (number of shares/contracts held)
- Beta coefficients (market correlation factors)

To calculate aggregate risk metrics:

- **Theo**: Total theoretical value of positions
- **DollarDelta**: Total $ exposure to underlying price moves (Delta × Position × Price)
- **BetaDollarDelta**: Market-correlated dollar exposure (adjusts for beta)
- **GammaPercent**: Delta stability risk (% change in delta for price moves)
- **Theta**: Daily time decay across all positions
- **VegaPercent**: Volatility risk (% sensitivity to vol changes)
- **Rho**: Interest rate risk
- **JumpUp10/JumpDown10**: Potential P&L from sudden 10% market moves

These metrics help portfolio managers understand total exposure and identify concentration risks.
"""

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