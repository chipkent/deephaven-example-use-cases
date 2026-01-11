# Basic Option Risk Management Example

This example demonstrates fundamental option risk management concepts in a single, self-contained script. All components are included inline for easy learning and experimentation.

## Overview

This example provides a complete but straightforward implementation of an option risk management system:
- Calculates option prices and Greeks using Black-Scholes
- Simulates market data for stocks and options
- Tracks portfolio positions from simulated trades
- Aggregates risk metrics at multiple levels
- Analyzes post-trade price movement

Everything runs in a single file with no external dependencies, making it ideal for learning and quick experimentation.

## Key Concepts

### Option Greeks
Sensitivity measures showing how option prices change with market conditions:
- **Delta**: Price sensitivity to underlying movement
- **Gamma**: Rate of change of delta
- **Theta**: Time decay (daily P&L from time passage)
- **Vega**: Sensitivity to volatility changes
- **Rho**: Sensitivity to interest rate changes

### Risk Aggregation
Portfolio risk can be viewed at multiple levels:
- **By position**: Individual security risk (`risk_all`)
- **By symbol + expiration**: Risk per underlying and maturity (`risk_ue`)
- **By symbol**: Total risk per underlying (`risk_u`)
- **By expiration**: Risk grouped by maturity date (`risk_e`)
- **Portfolio total**: Net risk across all positions (`risk_net`)

### Trade Analysis
The example includes post-trade analysis that measures price movement 10 minutes after execution, helping assess execution quality and short-term market impact.

## Running the Example

Simply execute [./risk_management.py](./risk_management.py) in Deephaven. The script will:
1. Define Black-Scholes pricing functions
2. Generate simulated market data (prices and volatility)
3. Create options at multiple strikes and expiries
4. Simulate random trades
5. Calculate Greeks for all positions
6. Aggregate risk at multiple levels
7. Analyze post-trade price movements

## Output Tables

### Market Data
- **`securities`**: Master list of tradeable stocks and options
- **`price_history`**: Streaming simulated prices and volatility
- **`price_current`**: Latest prices for all securities

### Greeks
- **`greek_history`**: Historical Greeks calculations (every 5 seconds)
- **`greek_current`**: Current Greeks for all securities

### Trading
- **`trade_history`**: All simulated trades with execution prices
- **`portfolio_history`**: Position changes over time
- **`portfolio_current`**: Current net positions by security

### Risk Metrics
- **`betas`**: Beta coefficients for market correlation
- **`risk_all`**: Detailed risk per position with:
  - Theo: Theoretical value
  - DollarDelta: $ exposure to price moves
  - BetaDollarDelta: Market-correlated exposure
  - GammaPercent: Delta stability risk
  - Theta: Daily time decay
  - VegaPercent: Volatility risk
  - Rho: Interest rate risk
  - JumpUp10/JumpDown10: 10% shock scenarios

### Risk Aggregations
- **`risk_ue`**: Risk by symbol and expiration
- **`risk_u`**: Risk by symbol
- **`risk_e`**: Risk by expiration
- **`risk_net`**: Total portfolio risk

### Trade Analysis
- **`trade_pnl`**: Post-trade price movement (10-minute forward look)
- **`trade_pnl_by_sym`**: P&L aggregated by symbol

## Comparison to Other Examples

This example and the other option risk examples demonstrate different approaches:

**Compared to simple_risk_management:**

### Use basic_option_risk for:
- **Learning**: See the complete workflow in one place
- **Quick prototyping**: No setup files or external dependencies
- **Simple analysis**: Multiple aggregation views of risk
- **Understanding fundamentals**: Clear, linear flow through the process

### Use simple_risk_management for:
- **Production monitoring**: Interactive dashboards with reactive filters
- **Multi-account portfolios**: Track risk by trading account
- **Real-time alerts**: Table listeners for risk threshold violations
- **Hierarchical analysis**: Rollup tables for drill-down exploration
- **Modular structure**: Separate setup file for reusability

**Compared to option_risk:**

### Use basic_option_risk for:
- **Learning fundamentals**: Simple, linear workflow
- **Quick setup**: Everything in one file
- **Basic aggregations**: Multiple risk views without complexity

### Use option_risk for:
- **Modular learning**: Step-by-step progression through components
- **Comprehensive coverage**: Includes slippage analysis and risk rollup
- **Code organization**: Separate files for each major component

## Customization

### Change Symbols
Modify the `usyms` list (line 125) to analyze different securities.

### Adjust Market Data Frequency
- Price updates: Change `"PT00:00:00.1"` (line 159) for different tick rates
- Greeks updates: Change `"PT00:00:05"` (line 257) for different calculation frequency
- Trade frequency: Change `"PT00:00:01"` (line 292) for different trade rates

### Modify Option Strikes
The `compute_strikes()` function (line 181) creates 10 strikes around the current price. Adjust the range or step size for different strike distributions.

### Change Risk-Free Rate
Modify `rate_risk_free` (line 210) to match current interest rate environment.

### Adjust Post-Trade Analysis Window
Change `"PT10m"` (line 371) to analyze price movement over different time periods after trades.

## Understanding the Output

### Interpreting Greeks
- **High Delta**: Position moves strongly with underlying price
- **High Gamma**: Delta is unstable, position risk changes rapidly
- **Negative Theta**: Position loses value over time (common for long options)
- **High Vega**: Position is sensitive to volatility changes
- **Rho**: Usually small unless interest rates are volatile

### Risk Aggregation Hierarchy
Start with `risk_net` to see total portfolio risk, then drill down:
1. `risk_u` - Which symbols have the most exposure?
2. `risk_ue` - How is risk distributed across expiries?
3. `risk_all` - What are the individual position risks?

### Jump Risk Interpretation
- **JumpUp10/JumpDown10**: Shows P&L if underlying jumps 10% instantly
- Large negative JumpDown10: Portfolio would lose if markets drop sharply
- Use this to identify concentration risk and tail risk exposure

## Technical Notes

### Simulation Details
- **Random walk prices**: Simple +/- random price changes
- **Volatility simulation**: Slowly changing implied volatility
- **Trade generation**: Random mix of stocks (30%) and options (70%)
- **Option strikes**: 10 strikes centered around current price
- **Expiries**: 30 and 60 days out

### Black-Scholes Implementation
Uses Numba's `@vectorize` decorator for high-performance JIT compilation. This enables efficient calculation of Greeks across hundreds of options simultaneously.

### Data Quality
The simulation may generate some null prices if options go too far out-of-the-money or have calculation issues. These are naturally filtered in downstream calculations.

## Extending the Example

### Add Real Market Data
Replace the simulated data with actual price feeds by substituting the market data generation sections with data from CSV files or live feeds.

### Add More Risk Metrics
Extend the `risk_all` table with additional calculations like:
- Second-order Greeks (Vanna, Volga)
- Value at Risk (VaR)
- Expected Shortfall

### Create Visualizations
Add plots to visualize:
- Greeks over time
- Risk distribution by symbol
- P&L attribution

### Implement Risk Limits
Add checks that compare computed risks against predefined limits and flag violations.
