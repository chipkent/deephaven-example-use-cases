# Market Maker Example

This example demonstrates a simulated market maker that uses statistical predictions to execute trades on live market data.

## What is a Market Maker?

A market maker is a firm or individual that quotes both buy and sell prices for financial instruments, profiting from the bid-ask spread and providing liquidity to the market. This example implements a simplified market maker strategy that:
- Monitors bid/ask prices from live market data
- Uses exponential moving averages (EMA) to predict favorable entry/exit points
- Executes simulated trades when market prices cross prediction thresholds
- Manages risk through position limits

## Prerequisites

**This example requires Deephaven Enterprise** with access to FeedOS equity quote data. It can be adapted for Deephaven Community by replacing the FeedOS data source with a simulated or alternative market data feed.

## Trading Strategy

The market maker uses a mean-reversion strategy based on EMAs:

1. **Price Prediction**: Calculates an exponential moving average (EMA) and standard deviation (SD) of the mid-price over a 1-minute window
2. **Buy Signal**: When the ask price falls below (predicted price - 1 SD), indicating the stock may be undervalued
3. **Sell Signal**: When the bid price rises above (predicted price + 1 SD), indicating the stock may be overvalued
4. **Risk Management**: Position limits in dollars prevent excessive exposure to any single symbol

## Key Features

- **Interactive Controls**: The `controls` table allows you to:
  - Add or remove symbols from the trading universe
  - Adjust maximum position size (in dollars) per symbol
- **Real-time Execution**: Trades are evaluated every 10 seconds based on current market conditions
- **Visualization**: Two plots show:
  - Price predictions with buy/sell thresholds
  - Actual trade executions overlaid on market data

## Output Tables

- **`controls`**: User-editable table to manage symbols and risk limits
- **`ticks_bid_ask`**: Live bid/ask quotes filtered for monitored symbols
- **`preds`**: Price predictions with EMA-based buy/sell thresholds
- **`trades`**: Historical record of all simulated trades
- **`positions`**: Current position (shares held) per symbol
- **`pnl`**: Unrealized profit and loss per symbol
- **`orders`**: Current trading signals and position status
- **`executions`**: Snapshot of recent trade decisions

## Running the Example

Execute [./market_maker.py](./market_maker.py) in a Deephaven Enterprise environment with FeedOS access. The script will begin monitoring the configured symbols (AAPL, GOOG, BAC by default) and executing trades when conditions are met.

## Backtesting

For backtesting this strategy across historical data, see the [replay_orchestration trading_simulation example](../../replay_orchestration/trading_simulation/), which adapts this market maker implementation to run across multiple dates with parallel workers.
