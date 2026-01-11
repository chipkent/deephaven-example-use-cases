############################################################################################################
# Interactive Stock Dashboard - Advanced Deephaven UI Patterns
#
# This example demonstrates production-quality dashboard development using Deephaven's UI framework.
# It showcases sophisticated reactive state management, performance optimization through memoization,
# and complex layout composition techniques essential for building interactive data applications.
#
# Key UI Patterns Demonstrated:
#
# 1. Reactive State Management
#    - Multiple independent state variables (symbol, exchange, window size, Bollinger bands)
#    - State propagation through component tree with automatic re-rendering
#    - Controlled components bound to state for synchronized UI behavior
#
# 2. Performance Optimization with Memoization
#    - ui.use_memo: Cache expensive computations (plots, table transformations)
#    - Dependency arrays: Precise control over when cached values recalculate
#    - Pre-computed rolling statistics reduce runtime calculations
#
# 3. Dynamic Data Binding
#    - ui.use_column_data: Extract unique values from tables to populate UI controls
#    - Data-driven UI: Pickers and selectors adapt automatically to table structure
#    - No hardcoded values: Dashboard adjusts to data changes
#
# 4. Complex Layout Composition
#    - Two-column responsive design (65% plot area, 35% control panel)
#    - Nested flex containers for precise component positioning
#    - Panel organization with logical grouping and titles
#
# 5. Real-Time Data Publishing
#    - table_publisher: Create blink tables for transient events
#    - blink_to_append_only: Convert ephemeral data to permanent history
#    - Event-driven updates: User actions trigger immediate data publishing
#
# Components:
# - line_plot: Multi-layer price chart (raw prices, rolling average, Bollinger bands)
# - filtered_table: Data table with dynamic filtering and column management
# - parameters_panel: Interactive controls (pickers, button groups)
# - orderbook_panel: Order simulation interface with buy/sell buttons
# - my_layout: Root component orchestrating entire dashboard
#
# Financial Features:
# - Bollinger Bands: Statistical volatility indicator with configurable confidence levels
# - Rolling Windows: Multiple time periods (5s, 30s, 1m, 5m) for moving averages
# - Exchange Filtering: View aggregated or exchange-specific data
# - Order Simulation: Interactive buy/sell interface with order history
#
# Output:
# - dashboard: ui.dashboard object containing the complete interactive interface
#   - Left panel: Price chart with technical indicators and data tables
#   - Right panel: Parameter controls and order book simulator
#
# Performance Strategy:
# - Pre-computation: Rolling statistics calculated once at startup (_stocks_with_stats)
# - Selective memoization: Only expensive operations cached with minimal dependencies
# - Layered plots: Multiple visualizations share underlying data efficiently
#
# Related Examples:
# - examples/finance/simple_risk_management: Risk dashboard with UI components
# - examples/finance/simulated_orders: Order management with table publishers
############################################################################################################

from deephaven import ui, agg, empty_table

from deephaven.stream.table_publisher import table_publisher
from deephaven.stream import blink_to_append_only

from deephaven.plot import express as dx
from deephaven import updateby as uby
from deephaven import dtypes as dht

############################################################################################################
# Data Source
############################################################################################################

# Load simulated stock data
stocks = dx.data.stocks().reverse()

############################################################################################################
# Helper Functions
############################################################################################################

def set_bol_properties(fig):
    """Configure Bollinger band plot appearance: hide legend and add shaded fill between bands."""
    fig.update_layout(showlegend=False)
    fig.update_traces(fill="tonexty", fillcolor='rgba(255,165,0,0.08)')

############################################################################################################
# UI Components - Visualization
############################################################################################################

@ui.component
def line_plot(
        filtered_source,
        exchange, window_size, bol_bands):
    """
    Renders price chart with three layers: base prices, rolling average, and Bollinger bands.
    Uses ui.use_memo to cache expensive plot generation.
    """

    # Map user-friendly labels to computed column names
    window_size_key = {
        "5 seconds": ("priceAvg5s", "priceStd5s"),
        "30 seconds": ("priceAvg30s", "priceStd30s"),
        "1 minute": ("priceAvg1m", "priceStd1m"),
        "5 minutes": ("priceAvg5m", "priceStd5m")}
    # 90th, 95th, 97.5th, and 99.5th percentiles of a standard normal distribution
    bol_bands_key = {"None": None, "80%": 1.282, "90%": 1.645, "95%": 1.960, "99%": 2.576}

    # Memoize plot generation - only recalculates when filtered_source or exchange changes
    base_plot = ui.use_memo(lambda: (
        dx.line(filtered_source, x="Timestamp", y="Price", by="Exchange" if exchange == "All" else None,
        unsafe_update_figure=lambda fig: fig.update_traces(opacity=0.4))
    ), [filtered_source, exchange])

    window_size_avg_key_col = window_size_key[window_size][0]
    window_size_std_key_col = window_size_key[window_size][1]

    avg_plot = ui.use_memo(lambda: dx.line(filtered_source,
        x="Timestamp", y=window_size_avg_key_col,
        color_discrete_sequence=["orange"],
        labels={window_size_avg_key_col: "Rolling Average"}),
        [filtered_source, window_size_avg_key_col]
    )

    bol_bands_key_col = bol_bands_key[bol_bands]

    # Conditionally create Bollinger band plot (None if bands disabled)
    bol_plot = ui.use_memo(lambda: (
        dx.line(filtered_source \
            .update([
                f"errorY={window_size_avg_key_col} + {bol_bands_key_col}*{window_size_std_key_col}",
                f"errorYMinus={window_size_avg_key_col} - {bol_bands_key_col}*{window_size_std_key_col}",
            ]),
        x="Timestamp", y=["errorYMinus", "errorY"],
        color_discrete_sequence=["rgba(255,165,0,0.3)", "rgba(255,165,0,0.3)"],
        unsafe_update_figure=set_bol_properties)
        if bol_bands_key_col is not None else None
    ), [filtered_source, window_size_avg_key_col, window_size_std_key_col, bol_bands_key_col])

    # Layer all plots together for combined visualization
    plot = ui.use_memo(lambda: dx.layer(base_plot, avg_plot, bol_plot), [base_plot, avg_plot, bol_plot])

    return ui.panel(plot, title="Prices")

@ui.component
def full_table(source):
    """Display the complete source data table with all columns."""
    return ui.panel(source, title="Full Table")

@ui.component
def filtered_table(source, exchange):
    """Shows filtered data table, removing computed columns for clarity."""
    if exchange == "All":
        return ui.panel(source \
            .drop_columns([
                "priceAvg5s", "priceStd5s", "priceAvg30s", "priceStd30s",
                "priceAvg1m", "priceStd1m", "priceAvg5m", "priceStd5m"]) \
            .reverse(), title="Filtered Table")
    return ui.panel(source \
        .drop_columns([
            "priceAvg5s", "priceStd5s", "priceAvg30s", "priceStd30s",
            "priceAvg1m", "priceStd1m", "priceAvg5m", "priceStd5m"]) \
        .where(f"Exchange == `{exchange}`")
        .reverse(), title="Filtered Table")

############################################################################################################
# UI Components - Controls and Input
############################################################################################################

@ui.component
def parameters_panel(
        symbols,
        exchanges,
        symbol, set_symbol,
        exchange, set_exchange,
        window_size, set_window_size,
        bol_bands, set_bol_bands):
    """
    Control panel with all interactive selectors for dashboard parameters.
    Includes pickers for symbol/exchange and button groups for window size/Bollinger bands.
    """

    symbol_picker = ui.picker(
        *symbols,
        label="Symbol",
        on_selection_change=set_symbol,
        selected_key=symbol,
    )
    exchange_picker = ui.picker(
        *exchanges,
        label="Exchange",
        on_selection_change=set_exchange,
        selected_key=exchange,
    )
    window_size_selector = ui.button_group(
        ui.button("5 seconds", variant="accent" if window_size == "5 seconds" else None, on_press=lambda: set_window_size("5 seconds")),
        ui.button("30 seconds", variant="accent" if window_size == "30 seconds" else None, on_press=lambda: set_window_size("30 seconds")),
        ui.button("1 minute", variant="accent" if window_size == "1 minute" else None, on_press=lambda: set_window_size("1 minute")),
        ui.button("5 minutes", variant="accent" if window_size == "5 minutes" else None, on_press=lambda: set_window_size("5 minutes")),
        margin_x=10
    )
    bollinger_band_selector = ui.button_group(
        ui.button("None", variant="accent" if bol_bands == "None" else None, on_press=lambda: set_bol_bands("None")),
        ui.button("80%", variant="accent" if bol_bands == "80%" else None, on_press=lambda: set_bol_bands("80%")),
        ui.button("90%", variant="accent" if bol_bands == "90%" else None, on_press=lambda: set_bol_bands("90%")),
        ui.button("95%", variant="accent" if bol_bands == "95%" else None, on_press=lambda: set_bol_bands("95%")),
        ui.button("99%", variant="accent" if bol_bands == "99%" else None, on_press=lambda: set_bol_bands("99%")),
        margin_x=10
    )

    return ui.panel(
        ui.flex(
            ui.flex(
                symbol_picker,
                exchange_picker,
                gap="size-200"
            ),
            ui.flex(
                ui.text("Window size:"),
                ui.flex(window_size_selector, direction="row"),
                gap="size-100",
                direction="column"
            ),
            ui.flex(
                ui.text("Bollinger bands:"),
                ui.flex(bollinger_band_selector, direction="row"),
                gap="size-100",
                direction="column"
            ),
            margin="size-200",
            direction="column",
            gap="size-200"
        ),
        title="Parameters"
    )

@ui.component
def orderbook_panel(symbols):
    """Order simulation interface with buy/sell buttons and order history table."""

    # Local state for this component only
    symbol, set_symbol = ui.use_state("")
    size, set_size = ui.use_state(0)

    # Create table publisher for order events (memoized to prevent recreation)
    blink_table, publisher = ui.use_memo(
        lambda: table_publisher(
            "Order table", {"sym": dht.string, "size": dht.int32, "side": dht.string}
        ),
        [],
    )
    # Convert blink table to append-only for permanent order history
    t = ui.use_memo(lambda: blink_to_append_only(blink_table), [blink_table])

    def submit_order(order_sym, order_size, side):
        publisher.add(
            empty_table(1).update(
                [f"sym=`{order_sym}`", f"size={order_size}", f"side=`{side}`"]
            )
        )

    def handle_buy(_):
        submit_order(symbol, size, "buy")

    def handle_sell(_):
        submit_order(symbol, size, "sell")

    symbol_picker = ui.picker(
        *symbols,
        label="Symbol",
        label_position="side",
        on_selection_change=set_symbol,
        selected_key=symbol
    )
    size_selector = ui.number_field(
        label="Size",
        label_position="side",
        value=size,
        on_change=set_size
    )

    return ui.panel(
        ui.flex(
            symbol_picker,
            size_selector,
            ui.button("Buy", on_press=handle_buy, variant="accent", style="fill"),
            ui.button("Sell", on_press=handle_sell, variant="negative", style="fill"),
            gap="size-200",
            margin="size-200",
            wrap=True,
        ),
        t,
        title="Order Book"
    )

############################################################################################################
# Main Dashboard Layout
############################################################################################################

@ui.component
def my_layout(source, source_with_stats):
    """Root component orchestrating the entire dashboard layout and state."""

    # Extract unique values from data to populate picker options dynamically
    symbols = ui.use_column_data(source.agg_by(agg.unique(cols="Sym"), by="Sym"))
    exchanges = ui.use_column_data(source.agg_by(agg.unique(cols="Exchange"), by="Exchange"))
    exchanges.append("All")

    # Dashboard state - each user selection has its own state variable
    symbol, set_symbol = ui.use_state(symbols[0])
    exchange, set_exchange = ui.use_state("All")
    window_size, set_window_size = ui.use_state("30 seconds")
    bol_bands, set_bol_bands = ui.use_state("90%")

    # Memoize table filtering to avoid recomputing when unrelated state changes
    # Only recalculates when symbol or source_with_stats changes
    single_symbol = ui.use_memo(lambda: (
            source_with_stats \
                .where([f"Sym == `{symbol}`"]) \
                .drop_columns([
                    "priceAvg5s", "priceStd5s", "priceAvg30s", "priceStd30s",
                    "priceAvg1m", "priceStd1m", "priceAvg5m", "priceStd5m"]) \
                .rename_columns([
                    "priceAvg5s=priceAvg5sAvg", "priceStd5s=priceStd5sAvg",
                    "priceAvg30s=priceAvg30sAvg", "priceStd30s=priceStd30sAvg",
                    "priceAvg1m=priceAvg1mAvg", "priceStd1m=priceStd1mAvg",
                    "priceAvg5m=priceAvg5mAvg", "priceStd5m=priceStd5mAvg"])
            if exchange == "All" else
                source_with_stats \
                    .where([f"Sym == `{symbol}`", f"Exchange == `{exchange}`"]) \
                    .drop_columns([
                        "priceAvg5sAvg", "priceStd5sAvg", "priceAvg30sAvg", "priceStd30sAvg",
                        "priceAvg1mAvg", "priceStd1mAvg", "priceAvg5mAvg", "priceStd5mAvg"]) \
        ), [symbol, source_with_stats]
    )

    return ui.row(
        ui.column(
            line_plot(
                single_symbol, exchange, window_size, bol_bands
            ),
            ui.stack(
                full_table(source),
                filtered_table(single_symbol, exchange),
            ),
            width=65
        ),
        ui.column(
            ui.row(
                parameters_panel(
                    symbols, exchanges,
                    symbol, set_symbol,
                    exchange, set_exchange,
                    window_size, set_window_size,
                    bol_bands, set_bol_bands
                ),
                height=40
            ),
            ui.row(
                orderbook_panel(symbols),
                height=60
            ),
            width=35
        )
    )

############################################################################################################
# Data Preparation and Dashboard Initialization
############################################################################################################

# Pre-compute rolling statistics at startup for better runtime performance
# Two separate update_by operations: one by Sym (all exchanges), one by Sym+Exchange
_sorted_stocks = stocks.sort("Timestamp")
_stocks_with_stats = _sorted_stocks \
    .update_by([
        uby.rolling_avg_time("Timestamp", "priceAvg5sAvg=Price", "PT2.5s", "PT2.5s"),
        uby.rolling_avg_time("Timestamp", "priceAvg30sAvg=Price", "PT15s", "PT15s"),
        uby.rolling_avg_time("Timestamp", "priceAvg1mAvg=Price", "PT30s", "PT30s"),
        uby.rolling_avg_time("Timestamp", "priceAvg5mAvg=Price", "PT150s", "PT150s"),
        uby.rolling_std_time("Timestamp", "priceStd5sAvg=Price", "PT2.5s", "PT2.5s"),
        uby.rolling_std_time("Timestamp", "priceStd30sAvg=Price", "PT15s", "PT15s"),
        uby.rolling_std_time("Timestamp", "priceStd1mAvg=Price", "PT30s", "PT30s"),
        uby.rolling_std_time("Timestamp", "priceStd5mAvg=Price", "PT150s", "PT150s"),
    ], by = ["Sym"]) \
    .update_by([
        uby.rolling_avg_time("Timestamp", "priceAvg5s=Price", "PT2.5s", "PT2.5s"),
        uby.rolling_avg_time("Timestamp", "priceAvg30s=Price", "PT15s", "PT15s"),
        uby.rolling_avg_time("Timestamp", "priceAvg1m=Price", "PT30s", "PT30s"),
        uby.rolling_avg_time("Timestamp", "priceAvg5m=Price", "PT150s", "PT150s"),
        uby.rolling_std_time("Timestamp", "priceStd5s=Price", "PT2.5s", "PT2.5s"),
        uby.rolling_std_time("Timestamp", "priceStd30s=Price", "PT15s", "PT15s"),
        uby.rolling_std_time("Timestamp", "priceStd1m=Price", "PT30s", "PT30s"),
        uby.rolling_std_time("Timestamp", "priceStd5m=Price", "PT150s", "PT150s"),
    ], by = ["Sym", "Exchange"])

dashboard = ui.dashboard(my_layout(stocks, _stocks_with_stats))
