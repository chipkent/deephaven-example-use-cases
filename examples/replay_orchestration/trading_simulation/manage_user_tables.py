"""
User Table Management Script for Trading Simulation

This script is meant to be run in the Deephaven console to manage user tables
created by the trading simulation replay orchestration.

Functions:
- delete_all_tables(output_namespace): Delete all trading simulation user tables
- list_tables(output_namespace): List all trading simulation user tables and their partition counts
- get_table(table_name, output_namespace): Get a specific table for querying

Note: Modern Deephaven versions may auto-create tables on first write,
making create_empty_tables() unnecessary.
"""

from deephaven_enterprise import db

# Default namespace for output tables
DEFAULT_NAMESPACE = "ExampleReplayTradingSim"

# Table names used by the trading simulation
TABLE_NAMES = [
    "TradingSimTrades",
    "TradingSimPositions",
    "TradingSimPnl",
    "TradingSimPreds",
    "TradingSimOrders",
    "TradingSimExecutions",
    "TradingSimSummary"
]

def delete_all_tables(output_namespace=DEFAULT_NAMESPACE):
    """
    Delete all trading simulation user tables.
    
    Args:
        output_namespace: Deephaven namespace containing the tables (default: DEFAULT_NAMESPACE)
    
    This will remove all partitions and data for all trading simulation tables.
    Use with caution!
    """
    deleted_count = 0
    not_found_count = 0
    
    print("[INFO] Deleting all trading simulation user tables...")
    
    for table_name in TABLE_NAMES:
        try:
            db.delete_partitioned_table(output_namespace, table_name)
            print(f"[INFO] Deleted: {table_name}")
            deleted_count += 1
        except Exception as e:
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                print(f"[SKIP] Table not found: {table_name}")
                not_found_count += 1
            else:
                print(f"[ERROR] Failed to delete {table_name}: {e}")
    
    print(f"\n[SUMMARY] Deleted: {deleted_count}, Not found: {not_found_count}, Total: {len(TABLE_NAMES)}")

def list_tables(output_namespace=DEFAULT_NAMESPACE):
    """
    List all trading simulation user tables and show partition information.
    
    Args:
        output_namespace: Deephaven namespace containing the tables (default: DEFAULT_NAMESPACE)
    """
    print("[INFO] Trading simulation user tables:")
    print("-" * 80)
    
    found_count = 0
    
    for table_name in TABLE_NAMES:
        try:
            table = db.get_partitioned_table(output_namespace, table_name)
            row_count = table.size
            print(f"  {table_name}: {row_count:,} rows")
            found_count += 1
        except Exception as e:
            if "not found" in str(e).lower() or "does not exist" in str(e).lower():
                print(f"  {table_name}: <not created>")
            else:
                print(f"  {table_name}: <error: {e}>")
    
    print("-" * 80)
    print(f"[SUMMARY] Found {found_count}/{len(TABLE_NAMES)} tables")

def get_table(table_name, output_namespace=DEFAULT_NAMESPACE):
    """
    Get a specific user table for querying.
    
    Args:
        table_name: One of the trading simulation table names
        output_namespace: Deephaven namespace containing the tables (default: DEFAULT_NAMESPACE)
        
    Returns:
        Deephaven table or None if not found
        
    Example:
        trades = get_table("TradingSimTrades")
        trades.head(100)
    """
    if table_name not in TABLE_NAMES:
        print(f"[ERROR] Unknown table: {table_name}")
        print(f"[INFO] Valid tables: {', '.join(TABLE_NAMES)}")
        return None
    
    try:
        table = db.get_partitioned_table(output_namespace, table_name)
        print(f"[INFO] Retrieved {table_name}: {table.size:,} rows")
        return table
    except Exception as e:
        print(f"[ERROR] Failed to get {table_name}: {e}")
        return None

# Print usage on load
print(f"""
================================================================================
Trading Simulation User Table Management
================================================================================

Available functions:

  delete_all_tables(output_namespace)  - Delete all trading simulation user tables
  list_tables(output_namespace)        - List all tables with row counts
  get_table(table_name, output_namespace) - Get a specific table for querying

Default namespace: "{DEFAULT_NAMESPACE}"

Table names:
  - TradingSimTrades
  - TradingSimPositions
  - TradingSimPnl
  - TradingSimPreds
  - TradingSimOrders
  - TradingSimExecutions
  - TradingSimSummary

Example usage:

  # List all tables (uses default namespace)
  list_tables()
  
  # List tables in custom namespace
  list_tables("MyCustomNamespace")
  
  # Get and query trades table
  trades = get_table("TradingSimTrades")
  trades.where("Sym = `AAPL`").tail(100)
  
  # Delete everything (CAREFUL!)
  delete_all_tables()

================================================================================
""")
