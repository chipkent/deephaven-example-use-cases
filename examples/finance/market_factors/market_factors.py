############################################################################################################
# Market Factor Analysis using Principal Component Analysis (PCA)
#
# This example demonstrates how to extract the dominant market factors driving stock returns using PCA.
# Market factors represent common patterns of co-movement across stocks (e.g., sector trends, market-wide
# movements, risk factors).
#
# What is PCA?
# Principal Component Analysis is a dimensionality reduction technique that identifies the directions
# (components) of maximum variance in the data. In finance, these components often represent systematic
# risk factors that affect groups of stocks simultaneously.
#
# Use Cases:
# - Portfolio risk management: Understand which market factors drive portfolio exposure
# - Factor-based trading: Identify and trade on dominant market themes
# - Risk decomposition: Separate systematic (market) risk from idiosyncratic (stock-specific) risk
# - Anomaly detection: Identify stocks deviating from common market patterns
#
# Implementation:
# This example uses scikit-learn's PCA implementation integrated with Deephaven tables via the
# deephaven.numpy module. It computes factors from historical stock returns and reports the explained
# variance for each component.
#
# Structure:
# 1. compute_factors(): Reusable function for computing PCA factors from price data
# 2. Example usage: Demonstrates the function on Deephaven Enterprise with FeedOS historical data
#
# Prerequisites:
# - Deephaven Enterprise with FeedOS historical trade data
# - scikit-learn (sklearn) and numpy packages
#
# Output:
# - factors table: Principal components (factor loadings) for each symbol
# - explained variance: How much variance each factor explains
# - cumulative variance: Running total of explained variance
############################################################################################################

from typing import Sequence, Tuple
import numpy as np
import numpy.typing as npt
from deephaven.numpy import to_numpy
from sklearn.decomposition import PCA
from deephaven import merge
from deephaven.table import Table
from deephaven.table_factory import new_table
from deephaven.column import string_col, double_col


def compute_factors(
        prices: Table,
        times: Table,
        symbols: Sequence[str],
        n_components: int,
        large_move_cutoff: float=0.01,
) -> Tuple[Table, npt.NDArray[np.float64], npt.NDArray[np.float64]]:
    """ Compute the PCA factors for the given symbols and times.

    Args:
        prices: A table containing the price data for the given symbols.  Must contain columns "Timestamp", and "Sym".
        times: A table containing the times to compute the factors.  Must contain a column "Timestamp".
        symbols: A sequence of symbols to compute the factors for.
        n_components: The number of components to compute.
        large_move_cutoff: The cutoff for large idiosyncratic moves.  Defaults to 0.01.

    Returns:
        A table containing the computed factors, a numpy array of the explained variance ratios, and a numpy array of the cumulative explained variance ratios.
    """
    symbols = symbols.copy()
    prices_wide = times.view("Timestamp")

    removes = []

    for sym in symbols:
        prices_wide = prices_wide \
            .aj(prices.where("Sym == sym"), ["Timestamp"], f"{sym}=Price") \
            .update(f"IsOk=isFinite({sym})")

        ok_size = prices_wide.where("IsOk").size

        if ok_size < times.size * 0.9:
            print(f"Insufficient data for symbol {sym}: {ok_size} of {times.size} valid")
            removes.append(sym)
        else:
            print(f"Sufficient data for symbol {sym}: {ok_size} of {times.size} valid")

    for sym in removes:
        prices_wide = prices_wide.drop_columns(sym)
        symbols.remove(sym)

    prices_wide = prices_wide.drop_columns("IsOk")

    deltas = prices_wide \
        .select(["Timestamp"] + [f"{sym} = log({sym}/{sym}_[ii-1])" for sym in symbols]) \
        .where([f"isFinite({sym})" for sym in symbols]) \
        .where("Timestamp - Timestamp_[ii-1] < 8*HOUR") \
        .drop_columns("Timestamp")

    # Filter out large idiosyncratic moves

    deltas1 = merge([deltas.view(f"Delta=abs({sym})") for sym in symbols]) \
        .sort_descending("Delta") \
        .head_pct(large_move_cutoff) \
        .tail(1)

    delta_cutoff = float(to_numpy(deltas1, ["Delta"])[0][0])

    deltas = deltas \
        .where([f"abs({sym}) < delta_cutoff" for sym in symbols]) \

    returns = to_numpy(deltas)

    # Subtract the mean
    demeaned_returns = returns - returns.mean()

    pca = PCA(n_components=n_components)
    pca.fit(demeaned_returns)
    pca_components = pca.components_

    factors = new_table(
        [string_col("Sym", symbols)] +
        [double_col(f"Factor{i}", pca_components[i]) for i in range(n_components)]
    )

    pct = pca.explained_variance_ratio_
    cum_pct = np.cumsum(pct)

    return factors, pct, cum_pct



############################################################################################################\n# Example Usage on Deephaven Enterprise\n#\n# This section demonstrates using the compute_factors() function with FeedOS historical trade data.\n# It analyzes 500+ stocks over a date range to extract the top market factors.\n#\n# Requirements:\n# - Deephaven Enterprise with FeedOS access\n# - Historical trade data for the specified date range\n#\n# The symbol list below contains common US equities. You can modify this list to analyze different\n# securities or markets.\n############################################################################################################

date_min = "2023-07-01"
# date_min = "2024-01-28"
date_max = "2024-01-31"
n_components = 10
symbols = [
'MMM',
'AOS',
'ABT',
'ABBV',
'ACN',
'ATVI',
'AYI',
'ADBE',
'AAP',
'AMD',
'AES',
'AET',
'AMG',
'AFL',
'A',
'APD',
'AKAM',
'ALK',
'ALB',
'ARE',
'ALXN',
'ALGN',
'ALLE',
'AGN',
'ADS',
'LNT',
'ALL',
'GOOGL',
'GOOG',
'MO',
'AMZN',
'AEE',
'AAL',
'AEP',
'AXP',
'AIG',
'AMT',
'AWK',
'AMP',
'ABC',
'AME',
'AMGN',
'APH',
'APC',
'ADI',
'ANDV',
'ANSS',
'ANTM',
'AON',
'APA',
'AIV',
'AAPL',
'AMAT',
'APTV',
'ADM',
'ARNC',
'AJG',
'AIZ',
'T',
'ADSK',
'ADP',
'AZO',
'AVB',
'AVY',
'BHGE',
'BLL',
'BAC',
'BAX',
'BBT',
'BDX',
'BBY',
'BIIB',
'BLK',
'HRB',
'BA',
'BWA',
'BXP',
'BSX',
'BHF',
'BMY',
'AVGO',
'CHRW',
'CA',
'COG',
'CDNS',
'CPB',
'COF',
'CAH',
'KMX',
'CCL',
'CAT',
'CBOE',
'CBG',
'CBS',
'CELG',
'CNC',
'CNP',
'CTL',
'CERN',
'CF',
'SCHW',
'CHTR',
'CHK',
'CVX',
'CMG',
'CB',
'CHD',
'CI',
'XEC',
'CINF',
'CTAS',
'CSCO',
'C',
'CFG',
'CTXS',
'CME',
'CMS',
'KO',
'CTSH',
'CL',
'CMCSA',
'CMA',
'CAG',
'CXO',
'COP',
'ED',
'STZ',
'GLW',
'COST',
'COTY',
'CCI',
'CSRA',
'CSX',
'CMI',
'CVS',
'DHI',
'DHR',
'DRI',
'DVA',
'DE',
'DAL',
'XRAY',
'DVN',
'DLR',
'DFS',
'DISCA',
'DISCK',
'DISH',
'DG',
'DLTR',
'D',
'DOV',
'DWDP',
'DPS',
'DTE',
'DUK',
'DRE',
'DXC',
'ETFC',
'EMN',
'ETN',
'EBAY',
'ECL',
'EIX',
'EW',
'EA',
'EMR',
'ETR',
'EVHC',
'EOG',
'EQT',
'EFX',
'EQIX',
'EQR',
'ESS',
'EL',
'RE',
'ES',
'EXC',
'EXPE',
'EXPD',
'ESRX',
'EXR',
'XOM',
'FFIV',
'FB',
'FAST',
'FRT',
'FDX',
'FIS',
'FITB',
'FE',
'FISV',
'FLIR',
'FLS',
'FLR',
'FMC',
'FL',
'F',
'FTV',
'FBHS',
'BEN',
'FCX',
'GPS',
'GRMN',
'IT',
'GD',
'GE',
'GGP',
'GIS',
'GM',
'GPC',
'GILD',
'GPN',
'GS',
'GT',
'GWW',
'HAL',
'HBI',
'HOG',
'HRS',
'HIG',
'HAS',
'HCA',
'HCP',
'HP',
'HSIC',
'HES',
'HPE',
'HLT',
'HOLX',
'HD',
'HON',
'HRL',
'HST',
'HPQ',
'HUM',
'HBAN',
'HII',
'IDXX',
'INFO',
'ITW',
'ILMN',
'INCY',
'IR',
'INTC',
'ICE',
'IBM',
'IP',
'IPG',
'IFF',
'INTU',
'ISRG',
'IVZ',
'IQV',
'IRM',
'JBHT',
'JEC',
'SJM',
'JNJ',
'JCI',
'JPM',
'JNPR',
'KSU',
'K',
'KEY',
'KMB',
'KIM',
'KMI',
'KLAC',
'KSS',
'KHC',
'KR',
'LB',
'LLL',
'LH',
'LRCX',
'LEG',
'LEN',
'LUK',
'LLY',
'LNC',
'LKQ',
'LMT',
'L',
'LOW',
'LYB',
'MTB',
'MAC',
'M',
'MRO',
'MPC',
'MAR',
'MMC',
'MLM',
'MAS',
'MA',
'MAT',
'MKC',
'MCD',
'MCK',
'MDT',
'MRK',
'MET',
'MTD',
'MGM',
'KORS',
'MCHP',
'MU',
'MSFT',
'MAA',
'MHK',
'TAP',
'MDLZ',
'MON',
'MNST',
'MCO',
'MS',
'MSI',
'MYL',
'NDAQ',
'NOV',
'NAVI',
'NTAP',
'NFLX',
'NWL',
'NFX',
'NEM',
'NWSA',
'NWS',
'NEE',
'NLSN',
'NKE',
'NI',
'NBL',
'JWN',
'NSC',
'NTRS',
'NOC',
'NCLH',
'NRG',
'NUE',
'NVDA',
'ORLY',
'OXY',
'OMC',
'OKE',
'ORCL',
'PCAR',
'PKG',
'PH',
'PDCO',
'PAYX',
'PYPL',
'PNR',
'PBCT',
'PEP',
'PKI',
'PRGO',
'PFE',
'PCG',
'PM',
'PSX',
'PNW',
'PXD',
'PNC',
'RL',
'PPG',
'PPL',
'PX',
'PCLN',
'PFG',
'PG',
'PGR',
'PLD',
'PRU',
'PEG',
'PSA',
'PHM',
'PVH',
'QRVO',
'QCOM',
'PWR',
'DGX',
'RRC',
'RJF',
'RTN',
'O',
'RHT',
'REG',
'REGN',
'RF',
'RSG',
'RMD',
'RHI',
'ROK',
'COL',
'ROP',
'ROST',
'RCL',
'SPGI',
'CRM',
'SBAC',
'SCG',
'SLB',
'SNI',
'STX',
'SEE',
'SRE',
'SHW',
'SIG',
'SPG',
'SWKS',
'SLG',
'SNA',
'SO',
'LUV',
'SWK',
'SBUX',
'STT',
'SRCL',
'SYK',
'STI',
'SYMC',
'SYF',
'SNPS',
'SYY',
'TROW',
'TPR',
'TGT',
'TEL',
'FTI',
'TXN',
'TXT',
'BK',
'CLX',
'COO',
'HSY',
'MOS',
'TRV',
'DIS',
'TMO',
'TIF',
'TWX',
'TJX',
'TMK',
'TSS',
'TSCO',
'TDG',
'TRIP',
'FOXA',
'FOX',
'TSN',
'USB',
'UDR',
'ULTA',
'UAA',
'UA',
'UNP',
'UAL',
'UNH',
'UPS',
'URI',
'UTX',
'UHS',
'UNM',
'VFC',
'VLO',
'VAR',
'VTR',
'VRSN',
'VRSK',
'VZ',
'VRTX',
'VIAB',
'V',
'VNO',
'VMC',
'WMT',
'WBA',
'WM',
'WAT',
'WEC',
'WFC',
'HCN',
'WDC',
'WU',
'WRK',
'WY',
'WHR',
'WMB',
'WLTW',
'WYN',
'WYNN',
'XEL',
'XRX',
'XLNX',
'XL',
'XYL',
'YUM',
'ZBH',
'ZION',
'ZTS'
]


import deephaven.dtypes as dht

symbols_array = dht.array(dht.string, symbols)

trades = db.historical_table("FeedOS", "EquityTradeL1_5Min") \
    .where(["inRange(Date, date_min, date_max)", "LocalCodeStr in symbols_array"]) \
    .view(["Date", "Timestamp", "Sym=LocalCodeStr", "Price=LastPrice"])

times = trades.view("Timestamp").select_distinct().sort("Timestamp")

factors, pct, cum_pct = compute_factors(trades, times, symbols, n_components)

print(f"Explained Variance: {pct}")
print(f"Cum Explained Variance: {cum_pct}")
