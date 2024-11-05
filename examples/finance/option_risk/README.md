# Option Risk System Example

This is an example of a simple option risk system. 
The system is designed to calculate the risk of an option and stock position based on the Greeks of the position. 
The Greeks are calculated using the Black-Scholes model.

The system is implemented in Python and uses the [`numpy`](https://numpy.org/) library for numerical calculations.
The Python [`numba`](https://deephaven.io/core/docs/how-to-guides/use-numba/) library is used to speed up the option model calculations.

The example can be run in one of two ways:
1. All code is combined into [./option_risk_combined.py](./option_risk_combined.py) and can be run as a single script.  This is a long file and can be more difficult to follow during a demo.
2. The code is broken down into smaller parts that can be run separately.  The smaller pieces can make the code easier to follow during a demo. Execute the files sequentially in the following order:
   1. [./option_risk_0_option_model.py](./option_risk_0_option_model.py) - The option model
   2. [./option_risk_1_sec_master.py](./option_risk_1_sec_master.py) - Simulate a security master
   3. [./option_risk_2_betas.py](./option_risk_2_betas.py) - Simulate betas
   4. [./option_risk_3_market_prices.py](./option_risk_3_market_prices.py) - Simulate market prices
   5. [./option_risk_4_trades.py](./option_risk_4_trades.py) - Simulate trades
   6. [./option_risk_5_greeks.py](./option_risk_5_greeks.py) - Compute greeks
   7. [./option_risk_6_risk.py](./option_risk_6_risk.py) - Compute risk
   8. [./option_risk_7_risk_rollup.py](./option_risk_7_risk_rollup.py) - Compute risk rollup
   9. [./option_risk_9_slippage.py](./option_risk_9_slippage.py) - Compute execution slippage
   9. [./option_risk_8_main.py](./option_risk_9_main.py) - Main
