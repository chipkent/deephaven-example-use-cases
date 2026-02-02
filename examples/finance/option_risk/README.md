# Option Risk System Example

This example demonstrates a comprehensive option risk management system that calculates portfolio risk metrics using the Black-Scholes model to compute option Greeks and analyze exposure.

## Overview

The system simulates a trading environment with stocks and options, then calculates various risk measures:

- **Greeks**: Delta, Gamma, Theta, Vega, and Rho calculated using Black-Scholes
- **Portfolio Risk**: Aggregated risk metrics across all positions
- **Risk Rollup**: Hierarchical risk aggregation by symbol, expiration, strike, and parity
- **Slippage Analysis**: Post-trade price movement analysis to measure execution quality

## Implementation Details

The system uses:

- **Black-Scholes Model**: Industry-standard option pricing formulas
- **[`numpy`](https://numpy.org/)**: For numerical array operations
- **[`numba`](https://deephaven.io/core/docs/how-to-guides/use-numba/)**: JIT compilation for high-performance Greeks calculations

## Running the Example

The example can be run in two ways:

### Option 1: Combined Script (Recommended for Quick Start)

Copy the contents of [`option_risk_combined.py`](./option_risk_combined.py) and paste it into the Deephaven console. This script contains all functionality and runs everything at once.

### Option 2: Step-by-Step Modules (Recommended for Learning/Demos)

Execute the files sequentially to see each component build up the system:

1. [./option_risk_0_option_model.py](./option_risk_0_option_model.py) - Black-Scholes pricing and Greeks functions
2. [./option_risk_1_sec_master.py](./option_risk_1_sec_master.py) - Security master table with stocks and options
3. [./option_risk_2_betas.py](./option_risk_2_betas.py) - Beta coefficients for market correlation
4. [./option_risk_3_market_prices.py](./option_risk_3_market_prices.py) - Real-time simulated market data
5. [./option_risk_4_trades.py](./option_risk_4_trades.py) - Simulated trading activity
6. [./option_risk_5_greeks.py](./option_risk_5_greeks.py) - Greeks calculation for all securities
7. [./option_risk_6_risk.py](./option_risk_6_risk.py) - Portfolio risk metrics
8. [./option_risk_7_risk_rollup.py](./option_risk_7_risk_rollup.py) - Hierarchical risk aggregation
9. [./option_risk_8_slippage.py](./option_risk_8_slippage.py) - Execution slippage analysis
10. [./option_risk_9_main.py](./option_risk_9_main.py) - Orchestrates all components

## Output Tables

### Simulated Market Data

- **`sec_master`**: Master table of stocks and options with strikes and expiries
- **`betas`**: Beta coefficients for correlating risk to market movements
- **`prices_history`**: Real-time simulated bid/ask prices and volatility
- **`prices_current`**: Latest prices for all securities
- **`trade_history`**: Simulated trades with execution prices
- **`portfolio_history`**: Position changes over time
- **`portfolio_current`**: Current net positions

### Risk Analytics

- **`greeks_current`**: Current Greeks (Delta, Gamma, Theta, Vega, Rho) for all securities
- **`risk_all`**: Detailed risk breakdown for each position, including:
  - Theoretical value (Theo)
  - Dollar delta and beta-adjusted delta
  - Gamma (in percentage terms)
  - Theta (time decay)
  - Vega (volatility sensitivity)
  - Jump risk (up/down 10% scenarios)
- **`risk_rollup`**: Hierarchical rollup table aggregating risk by symbol, expiration, strike, and parity
- **`slippage`**: Post-trade analysis showing price movement after execution
