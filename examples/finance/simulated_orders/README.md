# Simulated Orders Example

This script simulates an order management system.  It is just intended to create example data for looking at orders changing state.
It is not intending to simulate trading.

The script to run is [./simulated_orders.py](./simulated_orders.py).

Possible order states are:
* PendingSubmit - waiting for submission
* Submitted - submitted but not filled
* PartialFilled - submitted and partially filled
* Filled - fully filled
* Canceled - canceled before being fully filled
