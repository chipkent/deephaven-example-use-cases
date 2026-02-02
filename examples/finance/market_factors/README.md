# Market Factor Analysis with PCA

This example demonstrates how to identify and analyze dominant market factors driving stock returns using Principal Component Analysis (PCA).

## What Are Market Factors?

Market factors are systematic patterns of co-movement that affect groups of securities simultaneously. Examples include:
- **Market-wide movements**: Overall market up/down trends
- **Sector trends**: Technology stocks moving together, energy sector correlation
- **Risk factors**: Value vs growth, large cap vs small cap, momentum
- **Economic factors**: Interest rate sensitivity, inflation exposure

By identifying these factors, traders and portfolio managers can:
- Better understand portfolio risk exposure
- Hedge systematic risks
- Identify anomalous stock behavior
- Build factor-based trading strategies

## What is PCA?

Principal Component Analysis is a dimensionality reduction technique that transforms correlated variables (stock returns) into uncorrelated components. These components:
- Are ordered by the amount of variance they explain
- Represent the dominant directions of movement in the data
- Often correspond to interpretable market factors

In this example, PCA extracts the top factors from hundreds of stock returns, revealing the main drivers of market movement.

## Prerequisites

**This example requires Deephaven Enterprise** with access to FeedOS historical trade data.

**Python packages:**
- scikit-learn (sklearn)
- numpy

These are typically pre-installed in Deephaven environments.

## How It Works

The `compute_factors()` function:

1. **Prepares data**: Creates a wide table with prices for all symbols at each timestamp
2. **Filters incomplete data**: Removes symbols with insufficient data coverage (< 90%)
3. **Calculates returns**: Computes log returns between consecutive prices
4. **Filters outliers**: Removes extreme idiosyncratic moves that could distort the analysis
5. **Demeanes returns**: Subtracts the mean to center the data
6. **Runs PCA**: Uses scikit-learn to extract principal components
7. **Returns factors**: Provides factor loadings and explained variance

### Parameters

```python
compute_factors(
    prices: Table,           # Price data with Timestamp, Sym, Price columns
    times: Table,            # Timestamps to analyze
    symbols: Sequence[str],  # List of symbols to include
    n_components: int,       # Number of factors to extract
    large_move_cutoff: float # Percentile cutoff for outlier filtering (default 0.01 = 1%)
)
```

### Returns

- **factors**: Table with factor loadings for each symbol (how much each stock is influenced by each factor)
- **explained_variance**: Array showing what percentage of total variance each factor explains
- **cumulative_variance**: Running total of explained variance

## Running the Example

Copy the contents of [`market_factors.py`](./market_factors.py) and paste it into the console of Deephaven Enterprise. The script:
1. Queries FeedOS for historical 5-minute trade data
2. Analyzes 500+ US equity symbols over the specified date range
3. Extracts the top 10 market factors
4. Prints explained variance statistics

### Interpreting Results

**Explained Variance** shows how much of the total market movement each factor captures:
```
Factor 0: 25% - Usually represents broad market movement
Factor 1: 15% - Often sector-specific (tech, finance, etc.)
Factor 2: 10% - Secondary market trends
...
```

**Cumulative Variance** shows the running total:
```
Factors 0-2: 50% - First 3 factors explain half of all movement
Factors 0-4: 70% - Top 5 factors capture most systematic risk
```

**Factor Loadings** in the factors table show each stock's sensitivity to each factor:
- High positive loading: Stock moves strongly with the factor
- Near zero: Stock is independent of this factor
- High negative loading: Stock moves opposite to the factor

## Customization

### Analyze Different Symbols

Modify the `symbols` list to analyze different securities or reduce to a specific sector.

### Change Date Range

Adjust `date_min` and `date_max` to analyze different time periods. Longer periods provide more stable factors but may miss regime changes.

### Extract More/Fewer Factors

Change `n_components` to extract more or fewer factors. Common choices:
- 3-5 factors: Capture major market themes
- 10-20 factors: Detailed factor decomposition
- 50+ factors: Research/academic analysis

### Adjust Outlier Filtering

The `large_move_cutoff` parameter controls how aggressively to filter extreme moves:
- 0.01 (1%): Default, removes most extreme moves
- 0.05 (5%): More permissive, keeps more data
- 0.001 (0.1%): Very aggressive filtering

## Use Cases

### Portfolio Risk Management
Decompose portfolio returns into factor exposures to understand which market factors drive your risk.

### Factor-Based Trading
Identify when specific factors are trending and construct portfolios with high exposure to those factors.

### Risk Attribution
Separate systematic (market factor) risk from idiosyncratic (stock-specific) risk for better risk reporting.

### Anomaly Detection
Identify stocks that deviate significantly from their expected factor behavior, potentially signaling trading opportunities or data issues.

### Regime Change Detection
Track how factor loadings and explained variance change over time to detect shifts in market structure.

## Technical Notes

### Data Quality
The function automatically filters out symbols with insufficient data coverage (< 90% of timestamps). This ensures robust factor estimation.

### Outlier Handling
Large idiosyncratic moves are filtered to prevent individual stock events from distorting market-wide factors. The cutoff is based on the absolute return magnitude across all stocks.

### Time Gap Handling
Returns are only calculated when consecutive prices are within 8 hours of each other, preventing overnight gaps from being treated as intraday returns.

### Demeaning
Returns are centered (mean subtracted) before PCA to focus the analysis on co-movement patterns rather than overall drift.

## Adapting for Deephaven Community

This example requires Enterprise for FeedOS access. To adapt for Community:
1. Replace the FeedOS query with a simulated data source or CSV import
2. Generate synthetic correlated returns with known factor structure
3. Use the `compute_factors()` function as-is with your data

The function itself works with any Deephaven table containing price data in the required format.
