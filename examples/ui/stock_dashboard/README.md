# Interactive Stock Dashboard

A comprehensive demonstration of advanced Deephaven UI patterns using real-time stock market data. This example showcases reactive state management, performance optimization, and complex dashboard layouts.

## Overview

This dashboard provides an interactive interface for analyzing stock prices with technical indicators. Users can:
- Select stocks and exchanges from dropdowns
- View price charts with Bollinger bands
- Adjust rolling window periods dynamically
- Configure Bollinger band confidence levels
- Simulate order placement (buy/sell)
- View filtered data tables in real-time

## Key UI Patterns Demonstrated

### Reactive State Management
- **`ui.use_state`**: Manages multiple interactive state variables (symbol, exchange, window size, Bollinger bands)
- **State propagation**: Changes flow through the component tree triggering reactive updates
- **Controlled components**: UI elements are bound to state variables

### Performance Optimization
- **`ui.use_memo`**: Caches expensive computations to prevent unnecessary recalculations
  - Plot generation only re-runs when dependencies change
  - Table transformations are memoized
  - Pre-computed rolling statistics improve responsiveness
- **Dependency arrays**: Precise control over when memoized values update

### Dynamic Data Binding
- **`ui.use_column_data`**: Automatically extracts unique values from tables to populate pickers
  - Symbol list dynamically generated from data
  - Exchange list built from available exchanges
- **No hardcoded values**: UI adapts to data structure

### Complex Layouts
- **Two-column design**: Wide plot/table area (65%) + narrow parameter panel (35%)
- **Nested flex containers**: Precise control over component positioning
- **Panel organization**: Logical grouping with titles
- **Stack component**: Tab-like interface for table switching

### Interactive Components
- **Pickers**: Dropdown selection for symbols and exchanges
- **Button groups**: Mutually exclusive selection with visual feedback (accent variant)
- **Number field**: Order size input with validation
- **Action buttons**: Buy (accent) and Sell (negative) with different styling

### Real-Time Data Publishing
- **`table_publisher`**: Creates blink table for order events
- **`blink_to_append_only`**: Converts transient events to permanent history
- **Event handlers**: Button presses trigger data publishing

## Financial Features

### Bollinger Bands
Statistical indicator showing price volatility:
- **Upper/Lower bands**: Calculated as rolling average ± (std deviation × confidence level)
- **Confidence levels**: 80%, 90%, 95%, 99% (corresponding to z-scores: 1.282, 1.645, 1.960, 2.576)
- **Visualization**: Shaded region between bands shows volatility envelope

### Rolling Windows
Multiple time periods for moving average and standard deviation:
- **5 seconds**: Ultra-short-term for high-frequency patterns
- **30 seconds**: Short-term momentum
- **1 minute**: Medium-term trends
- **5 minutes**: Longer-term patterns

### Exchange Filtering
- View data across all exchanges combined
- Filter to specific exchange for detailed analysis

## Running the Example

Copy the contents of [`stock_dashboard.py`](./stock_dashboard.py) and paste it into the Deephaven console. The dashboard will appear with:

1. **Left panel (65% width)**:
   - Line plot with price, rolling average, and Bollinger bands
   - Tabbed view of full source data and filtered data

2. **Right panel (35% width)**:
   - Symbol and exchange pickers
   - Window size button group
   - Bollinger band selector
   - Order book simulator with buy/sell buttons

### Using the Dashboard

1. **Select a stock**: Choose from the symbol dropdown
2. **Choose exchange**: Pick specific exchange or "All"
3. **Adjust window size**: Click button to change rolling period
4. **Toggle Bollinger bands**: Select confidence level or "None"
5. **Simulate orders**: Enter size and click Buy/Sell to add to order book

## Code Structure

### Components

#### `line_plot`
Renders the main price chart with three layers:
- **Base plot**: Raw price data (semi-transparent)
- **Average plot**: Rolling average line (orange)
- **Bollinger plot**: Upper/lower bands (shaded orange region)

Uses `dx.layer()` to combine multiple plots into one visualization.

#### `filtered_table`
Shows data table with computed rolling statistics removed for clarity:
- Conditionally filters by exchange if not viewing "All"
- Reverses rows to show most recent data first

#### `parameters_panel`
Control panel with all interactive selectors:
- Two pickers for symbol and exchange
- Two button groups for window size and Bollinger bands
- Uses flex layout with appropriate gaps and margins

#### `orderbook_panel`
Order simulation interface:
- Local state for symbol and size selection
- `table_publisher` creates blink table for order events
- Buy/Sell buttons with distinct styling (accent vs negative)
- Orders are published and accumulated in append-only table

#### `my_layout`
Root component orchestrating the entire dashboard:
- Extracts unique symbols/exchanges from source data
- Manages all state variables
- Uses `ui.use_memo` to optimize table filtering
- Arranges components in two-column layout

### Data Pipeline

```
stocks (dx.data.stocks())
  ↓ sort by Timestamp
_sorted_stocks
  ↓ compute rolling stats by Sym
  ↓ compute rolling stats by Sym + Exchange
_stocks_with_stats
  ↓ filter by user selections
single_symbol
  ↓ render in components
dashboard
```

## UI Patterns for Your Applications

### When to Use Memoization
- Expensive table operations (joins, aggregations, complex updates)
- Plot generation
- Any computation that depends on specific state variables

### State Management Strategy
- One `use_state` per independent user choice
- Derived state calculated in `use_memo` with dependency arrays
- Pass state setters down to child components

### Layout Best Practices
- Use `ui.row`/`ui.column` for basic structure
- Apply `ui.flex` when you need precise gap/margin control
- Set explicit widths for multi-column layouts
- Use panels to group related components with titles

### Performance Tips
- Pre-compute heavy calculations outside components (see `_stocks_with_stats`)
- Memoize plot generation with specific dependencies
- Use `unsafe_update_figure` for plot customization without re-rendering
- Keep dependency arrays minimal and precise

## Customization Ideas

### Add More Technical Indicators
- MACD (Moving Average Convergence Divergence)
- RSI (Relative Strength Index)
- Volume-weighted average price (VWAP)

### Extend the Order Book
- Add limit orders with price targets
- Show order history by symbol
- Calculate P&L from executed orders
- Add order cancellation

### Enhanced Filtering
- Date range selector
- Price range filters
- Volume filters
- Multi-symbol comparison

### Additional Visualizations
- Candlestick charts for OHLC data
- Volume bars below price chart
- Correlation heatmap across symbols
- Performance metrics panel

## Technical Notes

### Data Source
Uses `dx.data.stocks()` - Deephaven's built-in simulated stock data with columns:
- `Timestamp`: Time of trade
- `Sym`: Stock symbol
- `Exchange`: Trading venue
- `Price`: Trade price
- `Size`: Share quantity

### Rolling Statistics
Two separate `update_by` operations:
1. **By Sym**: Averages across all exchanges for a symbol
2. **By Sym + Exchange**: Separate statistics per exchange

This dual approach allows viewing both aggregated and exchange-specific data.

### Bollinger Band Math
```
upper_band = rolling_avg + (z_score × rolling_std)
lower_band = rolling_avg - (z_score × rolling_std)
```
Where z_score corresponds to desired confidence level (e.g., 1.960 for 95%).

### Performance Characteristics
- Pre-computation at startup reduces runtime calculations
- Memoization prevents redundant work during user interaction
- Layered plots share data to minimize memory usage
- Responsive UI updates even with large datasets

## Related Examples

- **`examples/finance/simple_risk_management/`**: Risk management dashboard with UI components
- **`examples/finance/simulated_orders/`**: Order management system with table publishers
- **Other UI examples** (to be added): Form validation, table interactions, custom visualizations
