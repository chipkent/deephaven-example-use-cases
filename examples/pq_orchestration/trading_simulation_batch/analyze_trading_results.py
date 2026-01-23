"""
Trading Simulation Results Analysis Script

This script provides quantitative analysis functions for trading simulation results.
Run this script in the Deephaven IDE console after the PQ orchestration completes
and the simulation output tables are available.

HOW TO USE:
    1. Run your trading simulation using the PQ orchestrator
    2. Wait for completion (check orchestrator logs)
    3. In Deephaven IDE console, run: exec(open('analyze_trading_results.py').read())
    4. Call analysis functions with your simulation name from config.yaml

Expected Input Tables (created by trading simulation in ExampleBatchTradingSim namespace):
    - TradingSimTrades: Individual trade executions
        Columns: Date, Timestamp, Sym, Price, Size, PartitionID, SimulationName
        Size is positive for buys, negative for sells
    
    - TradingSimPnl: Daily profit/loss by symbol
        Columns: Date, Sym, PnL, SimulationName
        PnL is the realized profit or loss for that symbol on that date
    
    - TradingSimPositions: Position snapshots showing holdings
        Columns: Sym, Position, SimulationName
        Position is the number of shares held (can be negative for short positions)
    
    - TradingSimSummary: High-level summary by symbol
        Columns: Sym, TradeCount, TotalShares, SimulationName

Analysis Functions:
    - get_summary(): Start here! High-level overview with best/worst performers
    - analyze_pnl(): Profit/loss analysis with risk metrics (Sharpe ratio, drawdown, win rate)
    - analyze_trades(): Trade execution statistics, turnover, buy/sell patterns
    - analyze_by_symbol(): Deep dive into a specific stock's performance
    - analyze_by_date(): Examine activity on a specific trading day
    - analyze_positions(): Position sizing and distribution analysis

Key Metrics Explained:
    - Sharpe Ratio: Risk-adjusted return (higher is better, >1 is good, >2 is excellent)
    - Max Drawdown: Largest peak-to-trough decline (lower absolute value is better)
    - Win Rate: Percentage of profitable days (50%+ is positive)
    - Turnover: Total dollar value traded (useful for transaction cost estimation)

Quick Start Example:
    sim_name = "trading_simulation_batch"  # From your config.yaml
    summary = get_summary(sim_name)
    top_performers = summary["top_performers"]  # Assigns table to variable, displays in UI
"""

from typing import Dict, Any
from deephaven import agg
from deephaven.updateby import cum_sum, cum_max
from deephaven.plot.figure import Figure

# Default namespace for output tables
DEFAULT_NAMESPACE = "ExampleBatchTradingSim"

# Default simulation name from config.yaml
DEFAULT_SIMULATION_NAME = "trading_simulation_batch"

def analyze_pnl(simulation_name: str, output_namespace: str = DEFAULT_NAMESPACE) -> Dict[str, Any]:
    """
    Analyze profit and loss with risk-adjusted metrics for a specific simulation.
    
    This is the most important analysis function for evaluating trading strategy performance.
    It calculates key risk metrics that professional traders use to assess strategy quality.
    
    Args:
        simulation_name: Name of the simulation to analyze (from your config.yaml 'name' field)
        output_namespace: Namespace where tables are stored (default: "ExampleBatchTradingSim")
        
    Returns:
        Dictionary containing the following items:
        
        - "pnl": Raw daily P&L data filtered to this simulation only
        
        - "by_symbol": Performance breakdown for each stock symbol, sorted by total profit:
            * TotalPnL: Total profit/loss for this symbol across all days
            * TradingDays: Number of days this symbol had P&L entries
            * WinningDays: Count of days with positive P&L (profit days)
            * LosingDays: Count of days with negative P&L (loss days)
            * WinRate: Percentage of profitable days (1.0 = 100% wins)
            * AvgWin: Average profit on winning days (higher is better)
            * AvgLoss: Average loss on losing days (closer to 0 is better)
        
        - "by_date": Daily aggregate P&L with running total:
            * DailyPnL: Total profit/loss across all symbols for this date
            * CumulativePnL: Running total of P&L over time (equity curve)
            * SymbolCount: Number of symbols traded on this date
        
        - "by_date_plot": Plot of cumulative P&L over time (equity curve)
        
        - "overall": Single-row summary with key performance metrics:
            * TotalPnL: Total profit/loss across entire simulation
            * AvgDailyPnL: Average daily profit/loss
            * StdDevDaily: Standard deviation of daily returns (volatility measure)
            * SharpeRatio: Risk-adjusted return (annualized). >1 is good, >2 is excellent
            * MaxDrawdown: Worst peak-to-trough decline. More negative = worse
            * WinRate: Percentage of profitable days (0.5 = 50%)
    
    Raises:
        Exception: If the analysis fails (e.g., simulation not found, missing tables)
    
    Example:
        result = analyze_pnl("my_simulation")
        print(f"Total P&L: {result['overall'].to_string()}")  # View summary stats
        top_symbols = result["by_symbol"].head(10)  # Top 10 performers
        equity_curve = result["by_date"]  # Visualize performance over time
    """
    print(f"[INFO] Analyzing P&L for simulation: {simulation_name}...")
    
    try:
        pnl = db.historical_table(output_namespace, "TradingSimPnl") \
            .where(f"SimulationName = `{simulation_name}`")
        
        # P&L by symbol with win/loss statistics
        pnl_by_symbol = pnl.update_view([
            "IsWin = PnL > 0",
            "IsLoss = PnL < 0",
            "WinningDay = IsWin ? 1 : 0",
            "LosingDay = IsLoss ? 1 : 0",
            "WinPnL = IsWin ? PnL : NULL_DOUBLE",
            "LossPnL = IsLoss ? PnL : NULL_DOUBLE"
        ]).agg_by([
            agg.sum_("TotalPnL=PnL"),
            agg.count_("TradingDays"),
            agg.sum_("WinningDays=WinningDay"),
            agg.sum_("LosingDays=LosingDay"),
            agg.avg("AvgPnL=PnL"),
            agg.avg("AvgWin=WinPnL"),
            agg.avg("AvgLoss=LossPnL")
        ], by=["Sym"]) \
        .update_view("WinRate = WinningDays / (double)(WinningDays + LosingDays)") \
        .sort_descending("TotalPnL")
        
        # Daily P&L with cumulative
        pnl_by_date = pnl.agg_by([
            agg.sum_("DailyPnL=PnL"),
            agg.count_distinct("SymbolCount=Sym")
        ], by=["Date"]) \
        .sort(["Date"]) \
        .update_by(ops=cum_sum(cols=["CumulativePnL=DailyPnL"]))
        
        # Overall statistics with risk metrics
        daily_pnl_for_stats = pnl.agg_by([agg.sum_("DailyPnL=PnL")], by=["Date"])
        
        total_pnl = daily_pnl_for_stats.update_view("WinningDay = DailyPnL > 0 ? 1 : 0").agg_by([
            agg.sum_("TotalPnL=DailyPnL"),
            agg.avg("AvgDailyPnL=DailyPnL"),
            agg.std("StdDevDaily=DailyPnL"),
            agg.min_("MinDailyPnL=DailyPnL"),
            agg.max_("MaxDailyPnL=DailyPnL"),
            agg.count_("TradingDays"),
            agg.sum_("WinningDays=WinningDay")
        ]).update_view([
            "WinRate = WinningDays / (double)TradingDays",
            "SharpeRatio = StdDevDaily > 0 ? (AvgDailyPnL / StdDevDaily) * sqrt(252.0) : 0.0"
        ])
        
        # Calculate max drawdown from cumulative P&L
        pnl_with_dd = pnl_by_date \
            .update_by(ops=cum_max(cols=["RunningMax=CumulativePnL"])) \
            .update_view("Drawdown = CumulativePnL - RunningMax")
        
        max_drawdown_table = pnl_with_dd.agg_by([agg.min_("MaxDrawdown=Drawdown")])
        
        # Join max drawdown into overall stats (both are single-row tables)
        total_pnl = total_pnl.join(table=max_drawdown_table, on=[])
        
        # Create cumulative P&L plot for by_date
        # Convert Date string to Instant for plotting (use NY timezone for market dates)
        pnl_by_date_for_plot = pnl_by_date.update("DateInstant = parseInstant(Date + `T16:00:00 ET`)")
        by_date_plot = Figure().plot_xy(
            series_name="Cumulative P&L",
            t=pnl_by_date_for_plot,
            x="DateInstant",
            y="CumulativePnL"
        ).show()
        
        print(f"[INFO] P&L Analysis Complete (with cumulative P&L plot)")
        
        return {
            "pnl": pnl,
            "by_symbol": pnl_by_symbol,
            "by_date": pnl_by_date,
            "by_date_plot": by_date_plot,
            "overall": total_pnl
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze P&L: {e}")
        raise

def analyze_trades(simulation_name: str, output_namespace: str = DEFAULT_NAMESPACE) -> Dict[str, Any]:
    """
    Analyze trading activity including turnover and execution patterns.
    
    Use this function to understand trading frequency, execution costs, and position changes.
    Helpful for identifying overtrading or execution quality issues.
    
    Args:
        simulation_name: Name of the simulation to analyze (from your config.yaml 'name' field)
        output_namespace: Namespace where tables are stored (default: "ExampleBatchTradingSim")
        
    Returns:
        Dictionary containing five Deephaven tables:
        
        - "trades": Raw trade records filtered to this simulation
        
        - "by_symbol": Trading activity by stock symbol, sorted by trade count:
            * TradeCount: Number of trades executed for this symbol
            * TotalShares: Total shares traded (absolute value, ignores buy/sell direction)
            * NetShares: Net position change (positive = net bought, negative = net sold)
            * AvgPrice: Average execution price across all trades
            * PriceRange: Difference between highest and lowest execution price
        
        - "by_date": Daily trading metrics:
            * TradeCount: Number of trades executed on this date
            * Volume: Total shares traded (sum of absolute values)
            * AvgTradeSize: Average shares per trade
            * Turnover: Total dollar value traded (sum of |price × size|). Use for cost estimation
        
        - "by_side": Buy vs Sell comparison:
            * Side: "BUY" or "SELL"
            * TradeCount: Number of trades in this direction
            * TotalShares: Total shares traded in this direction
        
        - "overall": Single-row aggregate statistics across all trades
    
    Raises:
        Exception: If the analysis fails (e.g., simulation not found, missing tables)
    
    Example:
        result = analyze_trades("my_simulation")
        by_date = result["by_date"]  # Daily turnover for cost analysis
        buy_sell = result["by_side"]  # Check if strategy is directionally biased
    """
    print(f"[INFO] Analyzing trades for simulation: {simulation_name}...")
    
    try:
        trades = db.historical_table(output_namespace, "TradingSimTrades") \
            .where(f"SimulationName = `{simulation_name}`") \
            .update_view(["AbsSize = abs(Size)", "Turnover = AbsSize * Price"])
        
        # Trade statistics by symbol with net position tracking
        trades_by_symbol = trades.agg_by([
            agg.count_("TradeCount"),
            agg.sum_("TotalShares=AbsSize"),
            agg.sum_("NetShares=Size"),
            agg.avg("AvgPrice=Price"),
            agg.min_("MinPrice=Price"),
            agg.max_("MaxPrice=Price")
        ], by=["Sym"]) \
        .update_view("PriceRange = MaxPrice - MinPrice") \
        .sort_descending("TradeCount")
        
        # Trade statistics by date with turnover
        trades_by_date = trades.agg_by([
            agg.count_("TradeCount"),
            agg.sum_("Volume=AbsSize"),
            agg.count_distinct("UniqueSymbols=Sym"),
            agg.sum_("Turnover")
        ], by=["Date"]) \
        .sort(["Date"])
        
        # Buy vs Sell analysis
        trades_with_side = trades.update_view("Side = Size > 0 ? `BUY` : `SELL`")
        trades_by_side = trades_with_side.agg_by([
            agg.count_("TradeCount"),
            agg.sum_("TotalShares=AbsSize")
        ], by=["Side"])
        
        # Overall statistics
        overall_stats = trades.agg_by([
            agg.count_("TotalTrades"),
            agg.sum_("TotalShares=AbsSize"),
            agg.count_distinct("UniqueSymbols=Sym"),
            agg.count_distinct("TradingDays=Date")
        ])
        
        print(f"[INFO] Trade Analysis Complete")
        
        return {
            "trades": trades,
            "by_symbol": trades_by_symbol,
            "by_date": trades_by_date,
            "by_side": trades_by_side,
            "overall": overall_stats
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze trades: {e}")
        raise

def analyze_by_symbol(simulation_name: str, symbol: str, output_namespace: str = DEFAULT_NAMESPACE) -> Dict[str, Any]:
    """
    Deep dive analysis for a specific symbol with risk metrics.
    
    Provides comprehensive performance analysis for individual symbols including
    trade-by-trade details, P&L evolution, and risk statistics.
    
    Args:
        simulation_name: Name of the simulation to analyze (required)
        symbol: Stock symbol to analyze (e.g., "AAPL")
        output_namespace: Deephaven namespace containing the tables (default: DEFAULT_NAMESPACE)
        
    Returns:
        Dictionary with the following keys:
        - "trades": All trades for this symbol
        - "trade_timeline": Chronological trade sequence (Date, Timestamp, Price, Size)
        - "pnl": P&L table for this symbol
        - "daily_pnl": Daily P&L with cumulative P&L
        - "positions": Position snapshots
        - "position_history": Position evolution
        - "stats": Summary statistics (TotalTrades, NetShares, AvgPrice, TotalPnL, 
                   WinRate, SharpeRatio)
    
    Raises:
        Exception: If the analysis fails (e.g., simulation not found, symbol not found)
    """
    print(f"[INFO] Analyzing symbol {symbol} for simulation: {simulation_name}...")
    
    try:
        # Get all tables for this symbol and simulation
        trades = db.historical_table(output_namespace, "TradingSimTrades") \
            .where([f"SimulationName = `{simulation_name}`", f"Sym = `{symbol}`"])
        
        pnl = db.historical_table(output_namespace, "TradingSimPnl") \
            .where([f"SimulationName = `{simulation_name}`", f"Sym = `{symbol}`"])
        
        positions = db.historical_table(output_namespace, "TradingSimPositions") \
            .where([f"SimulationName = `{simulation_name}`", f"Sym = `{symbol}`"])
        
        # Trade timeline
        trade_timeline = trades.sort(["Date", "Timestamp"]) \
            .view(["Date", "Timestamp", "Price", "Size"])
        
        # Daily P&L
        #TODO: remove the select() call once the issue is fixed (https://deephaven.atlassian.net/browse/DH-21462)
        daily_pnl = pnl.view(["Date", "Sym", "PnL"]) \
            .select() \
            .sort(["Date"])
        
        # Position history
        position_history = positions.view(["Sym", "Position"]) \
            .sort(["Sym"])
        
        # Statistics with win/loss analysis and risk metrics
        pnl_stats = pnl.update_view([
            "IsWin = PnL > 0",
            "WinningDay = IsWin ? 1 : 0"
        ]).agg_by([
            agg.sum_("TotalPnL=PnL"),
            agg.count_("TradingDays"),
            agg.sum_("WinningDays=WinningDay"),
            agg.avg("AvgDailyPnL=PnL"),
            agg.std("StdDevPnL=PnL")
        ]).update_view([
            "WinRate = WinningDays / (double)TradingDays",
            "SharpeRatio = StdDevPnL > 0 ? (AvgDailyPnL / StdDevPnL) * sqrt(252.0) : 0.0"
        ])
        
        trade_stats = trades.agg_by([
            agg.count_("TotalTrades"),
            agg.sum_("NetShares=Size"),
            agg.avg("AvgPrice=Price")
        ])
        
        # Combine trade and P&L stats (both are single-row tables)
        stats = trade_stats.join(table=pnl_stats, on=[])
        
        # Add cumulative P&L to daily_pnl
        daily_pnl_with_cumsum = daily_pnl.update_by(ops=cum_sum(cols=["CumulativePnL=PnL"]))
        
        print(f"[INFO] Symbol {symbol} Analysis Complete")
        
        return {
            "trades": trades,
            "trade_timeline": trade_timeline,
            "pnl": pnl,
            "daily_pnl": daily_pnl_with_cumsum,
            "positions": positions,
            "position_history": position_history,
            "stats": stats
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze symbol {symbol}: {e}")
        raise

def analyze_by_date(simulation_name: str, date: str, output_namespace: str = DEFAULT_NAMESPACE) -> Dict[str, Any]:
    """
    Analyze trading activity and performance for a specific date.
    
    Useful for investigating daily behavior and identifying unusual trading patterns.
    
    Args:
        simulation_name: Name of the simulation to analyze (required)
        date: Date to analyze in YYYY-MM-DD format (e.g., "2024-01-15")
        output_namespace: Deephaven namespace containing the tables (default: DEFAULT_NAMESPACE)
        
    Returns:
        Dictionary with the following keys:
        - "trades": All trades on this date
        - "trades_by_symbol": Per-symbol activity (TradeCount, TotalShares)
        - "pnl": P&L for this date
        - "pnl_by_symbol": Per-symbol P&L sorted by performance
        - "timeline": Chronological trade sequence (Timestamp, Sym, Price, Size)
        - "trade_stats": Overall statistics for the day
        - "pnl_stats": P&L statistics for the day
    
    Raises:
        Exception: If the analysis fails (e.g., simulation not found, date not found)
    """
    print(f"[INFO] Analyzing date {date} for simulation: {simulation_name}...")
    
    try:
        # Get all tables for this date and simulation
        trades = db.historical_table(output_namespace, "TradingSimTrades") \
            .where([f"SimulationName = `{simulation_name}`", f"Date = `{date}`"]) \
            .update_view("AbsSize = abs(Size)")
        
        pnl = db.historical_table(output_namespace, "TradingSimPnl") \
            .where([f"SimulationName = `{simulation_name}`", f"Date = `{date}`"])
        
        # Trade activity by symbol
        trades_by_symbol = trades.agg_by([
            agg.count_("TradeCount"),
            agg.sum_("TotalShares=AbsSize")
        ], by=["Sym"]) \
        .sort_descending("TradeCount")
        
        # P&L by symbol
        pnl_by_symbol = pnl.view(["Sym", "PnL"]) \
            .sort_descending("PnL")
        
        # Trade timeline
        trade_timeline = trades.sort(["Timestamp"]) \
            .view(["Timestamp", "Sym", "Price", "Size"])
        
        # Statistics
        stats = trades.agg_by([
            agg.count_("TotalTrades"),
            agg.count_distinct("UniqueSymbols=Sym"),
            agg.sum_("NetShares=Size")
        ])
        
        pnl_stats = pnl.agg_by([
            agg.sum_("TotalPnL=PnL"),
            agg.avg("AvgPnL=PnL")
        ])
        
        print(f"[INFO] Date {date} Analysis Complete")
        
        return {
            "trades": trades,
            "trades_by_symbol": trades_by_symbol,
            "pnl": pnl,
            "pnl_by_symbol": pnl_by_symbol,
            "timeline": trade_timeline,
            "trade_stats": stats,
            "pnl_stats": pnl_stats
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze date {date}: {e}")
        raise

def analyze_positions(simulation_name: str, output_namespace: str = DEFAULT_NAMESPACE) -> Dict[str, Any]:
    """
    Analyze position sizing and distribution across the simulation.
    
    Helps evaluate position management and sizing discipline. Useful for identifying
    concentration risk and ensuring appropriate position limits.
    
    Args:
        simulation_name: Name of the simulation to analyze (required)
        output_namespace: Deephaven namespace containing the tables (default: DEFAULT_NAMESPACE)
        
    Returns:
        Dictionary with the following keys:
        - "positions": All position snapshots
        - "final_positions": Ending positions (non-zero only), sorted by size
        - "by_symbol": Position statistics per symbol (AvgPosition, MinPosition, MaxPosition)
        - "overall": Aggregate position statistics (UniqueSymbols, AvgPosition, MaxPosition)
    
    Raises:
        Exception: If the analysis fails (e.g., simulation not found, missing tables)
    """
    print(f"[INFO] Analyzing positions for simulation: {simulation_name}...")
    
    try:
        positions = db.historical_table(output_namespace, "TradingSimPositions") \
            .where(f"SimulationName = `{simulation_name}`")
        
        # Final positions by symbol
        final_positions = positions.last_by(["Sym"]) \
            .view(["Sym", "Position"]) \
            .where("Position != 0") \
            .update_view("AbsPosition = abs(Position)") \
            .sort_descending("AbsPosition")
        
        # Position statistics
        position_stats = positions.agg_by([
            agg.avg("AvgPosition=Position"),
            agg.min_("MinPosition=Position"),
            agg.max_("MaxPosition=Position"),
            agg.count_("Observations")
        ], by=["Sym"]) \
        .update_view("AbsAvgPosition = abs(AvgPosition)") \
        .sort_descending("AbsAvgPosition")
        
        # Overall statistics
        overall_stats = positions.agg_by([
            agg.count_distinct("UniqueSymbols=Sym"),
            agg.avg("AvgPosition=abs(Position)"),
            agg.max_("MaxPosition=abs(Position)")
        ])
        
        print(f"[INFO] Position Analysis Complete")
        
        return {
            "positions": positions,
            "final_positions": final_positions,
            "by_symbol": position_stats,
            "overall": overall_stats
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to analyze positions: {e}")
        raise

def get_summary(simulation_name: str, output_namespace: str = DEFAULT_NAMESPACE) -> Dict[str, Any]:
    """
    Get high-level summary with performance rankings - START HERE!
    
    This is the recommended first function to call when analyzing simulation results.
    It provides a quick overview of what happened and highlights the best/worst performers.
    
    Args:
        simulation_name: Name of the simulation to analyze (from your config.yaml 'name' field)
        output_namespace: Namespace where tables are stored (default: "ExampleBatchTradingSim")
        
    Returns:
        Dictionary containing eight items (3 raw tables + 5 summary tables):
        
        - "trades": All trade records for this simulation (unfiltered raw data)
        - "pnl": All P&L records for this simulation (unfiltered raw data)
        - "summary": All summary records for this simulation (unfiltered raw data)
        
        - "trade_stats": Single-row summary of trading activity:
            * TotalTrades: Total number of trades executed
            * TotalShares: Total shares traded (sum of absolute values)
            * UniqueSymbols: Number of different stocks traded
            * TradingDays: Number of days with trading activity
        
        - "pnl_stats": Single-row summary of P&L performance:
            * TotalPnL: Total profit/loss across entire simulation
            * AvgPnL: Average P&L per record
            * MinPnL: Worst single P&L record (most negative)
            * MaxPnL: Best single P&L record (most positive)
        
        - "summary_stats": Single-row aggregate from summary table
        
        - "top_performers": Top 5 symbols ranked by total P&L (best performers)
        - "bottom_performers": Bottom 5 symbols ranked by total P&L (worst performers)
    
    Raises:
        Exception: If the analysis fails (e.g., simulation not found, missing tables)
    
    Example:
        summary = get_summary("my_simulation")
        
        # Quick overview
        print("Trade Stats:")
        print(summary["trade_stats"].to_string())
        
        # Identify winners and losers
        winners = summary["top_performers"]  # Investigate these further
        losers = summary["bottom_performers"]  # Understand what went wrong
    """
    print(f"[INFO] Generating summary for simulation: {simulation_name}...")
    
    try:
        # Get basic stats from each table for this simulation
        trades = db.historical_table(output_namespace, "TradingSimTrades") \
            .where(f"SimulationName = `{simulation_name}`") \
            .update_view("AbsSize = abs(Size)")
        pnl = db.historical_table(output_namespace, "TradingSimPnl") \
            .where(f"SimulationName = `{simulation_name}`")
        summary = db.historical_table(output_namespace, "TradingSimSummary") \
            .where(f"SimulationName = `{simulation_name}`")
        
        # Trade statistics
        trade_stats = trades.agg_by([
            agg.count_("TotalTrades"),
            agg.sum_("TotalShares=AbsSize"),
            agg.count_distinct("UniqueSymbols=Sym"),
            agg.count_distinct("TradingDays=Date")
        ])
        
        # P&L statistics
        pnl_stats = pnl.agg_by([
            agg.sum_("TotalPnL=PnL"),
            agg.avg("AvgPnL=PnL"),
            agg.min_("MinPnL=PnL"),
            agg.max_("MaxPnL=PnL")
        ])
        
        # Summary statistics
        summary_stats = summary.agg_by([
            agg.sum_("TotalTrades=TradeCount"),
            agg.sum_("TotalShares"),
            agg.count_distinct("UniqueSymbols=Sym")
        ])
        
        # Top performers
        top_pnl = pnl.agg_by([agg.sum_("TotalPnL=PnL")], by=["Sym"]) \
            .sort_descending("TotalPnL") \
            .head(5)
        
        # Bottom performers
        bottom_pnl = pnl.agg_by([agg.sum_("TotalPnL=PnL")], by=["Sym"]) \
            .sort(["TotalPnL"]) \
            .head(5)
        
        print(f"[INFO] Summary generation complete")
        
        return {
            "trades": trades,
            "pnl": pnl,
            "summary": summary,
            "trade_stats": trade_stats,
            "pnl_stats": pnl_stats,
            "summary_stats": summary_stats,
            "top_performers": top_pnl,
            "bottom_performers": bottom_pnl
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to generate summary: {e}")
        raise

# Print usage on load
print(f"""
================================================================================
Trading Simulation Results Analysis
================================================================================

QUICK START:
  1. Set your simulation name: sim_name = "trading_simulation_batch"  # or use DEFAULT_SIMULATION_NAME
  2. Get overview: summary = get_summary(sim_name)
  3. View results: summary["top_performers"]  # Displays in UI

All functions require a simulation_name parameter (from your config.yaml 'name').
Results are returned as dictionaries of Deephaven tables that auto-display when
assigned to variables.

AVAILABLE FUNCTIONS:

  get_summary(sim_name)           - START HERE! Overview + best/worst performers
  analyze_pnl(sim_name)           - P&L metrics (Sharpe, drawdown, win rate)
  analyze_trades(sim_name)        - Trade statistics, turnover, buy/sell patterns
  analyze_by_symbol(sim_name, sym)- Deep dive on a specific stock
  analyze_by_date(sim_name, date) - Analyze a specific trading day
  analyze_positions(sim_name)     - Position sizing and distribution

KEY METRICS:
  - Sharpe Ratio: Risk-adjusted return (>1 is good, >2 is excellent)
  - Max Drawdown: Worst decline from peak (more negative = worse)
  - Win Rate: % of profitable days (0.5 = 50%)
  - Turnover: Dollar value traded (for cost estimation)

Default namespace: "{DEFAULT_NAMESPACE}"

EXAMPLE WORKFLOW:

# Step 1: Get your simulation name from config.yaml
sim_name = DEFAULT_SIMULATION_NAME  # or "trading_simulation_batch"

# Step 2: Start with high-level summary
summary = get_summary(sim_name)
winners = summary["top_performers"]       # See best stocks
losers = summary["bottom_performers"]     # See worst stocks

# Step 3: Analyze overall performance
pnl = analyze_pnl(sim_name)
overall = pnl["overall"]                  # Sharpe, drawdown, win rate
by_date = pnl["by_date"]                  # Equity curve over time
by_date_plot = pnl["by_date_plot"]        # Cumulative P&L plot

# Step 4: Investigate specific stocks
aapl = analyze_by_symbol(sim_name, "AAPL")
aapl_stats = aapl["stats"]                # Performance summary
aapl_equity = aapl["daily_pnl"]           # P&L evolution

# Step 5: Analyze trading patterns
trades = analyze_trades(sim_name)
turnover = trades["by_date"]              # Daily costs
buy_sell = trades["by_side"]              # Direction bias

For detailed help, see the module docstring at the top of this file.

================================================================================
""")
