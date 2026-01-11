# Simple Risk Management Example

This example demonstrates a real-time risk management system for a portfolio of stocks and options, using the Black-Scholes model to calculate option Greeks and portfolio risk metrics.

## What is Risk Management?

Risk management in trading involves monitoring and controlling exposure to potential losses. This example calculates various risk metrics for a portfolio containing both stocks and options:

- **Greeks**: Sensitivity measures showing how option prices change with market conditions
- **Dollar Delta**: Total dollar exposure to underlying price movements
- **Jump Risk**: Potential profit/loss from sudden 10% price movements
- **Beta-Adjusted Risk**: Market-correlated risk exposure

## Greeks Explained

The system calculates the following Greeks using the Black-Scholes model:

- **Delta**: Sensitivity to underlying price changes (how much the option price moves per $1 change in stock)
- **Gamma**: Rate of change of delta (measures delta stability)
- **Theta**: Time decay (daily profit/loss from passage of time)
- **Vega**: Sensitivity to volatility changes
- **Rho**: Sensitivity to interest rate changes

## Key Features

- **Real-time Risk Calculation**: Continuously updates Greeks and risk metrics as market data changes
- **Risk Aggregation**: Rolls up risk across multiple dimensions (symbol, expiration, strike)
- **Jump Risk Analysis**: Calculates portfolio impact from sudden 10% market moves
- **Risk Alerts**: Triggers notifications when risk exceeds configured thresholds
- **Interactive UI**: Reactive interface for filtering and analyzing risk by account, symbol, and expiration

## Running the Example

Execute [./risk_management.py](./risk_management.py) in Deephaven. The script automatically downloads and imports [./setup_risk_management.py](./setup_risk_management.py), which:

- Implements Black-Scholes option pricing functions using Numba for performance
- Generates simulated market data (prices, volatility, trades)
- Creates test portfolio positions across multiple accounts

## Output Tables

- **`securities`**: Master table of available stocks and options (symbols, strikes, expiries)
- **`price_history`**: Simulated real-time price feed for all securities
- **`trade_history`**: Historical trades across all accounts
- **`portfolio_history`**: Position changes over time
- **`portfolio_current`**: Current positions by security
- **`greek_current`**: Current option Greeks and theoretical values
- **`betas`**: Beta coefficients for correlating risk to market movements
- **`risk_all`**: Detailed risk breakdown for each position
- **`risk_roll`**: Hierarchical risk aggregation (rollup table)
- **`jump`**: Aggregate jump risk by symbol
- **`jump_alerts`**: Symbols exceeding risk thresholds (triggers alerts)

## Visualizations

- **`jump_risk`**: Bar chart showing potential profit/loss from 10% market jumps by symbol
- **`reactive_risk`**: Interactive UI with filters for drilling down into risk by account, symbol, and expiration

## Risk Alert System

The example includes a table listener that monitors jump risk and prints alerts when thresholds are exceeded:

- Alerts trigger when any symbol's downside jump risk exceeds the configured limit
- Messages are printed to console for integration with external alerting systems (Slack, email, etc.)
- Use `handle.stop()` and `handle.start()` to pause/resume monitoring