# Simulated Orders Example

This example simulates a realistic order management system (OMS) that demonstrates how orders transition through various states in a trading system. The focus is on state management and data flow, not trading strategy.

## Overview

An order management system tracks the lifecycle of trading orders from creation to completion. This example simulates:

- **Order Creation**: New orders are continuously generated
- **State Transitions**: Orders progress through submission, filling, and completion
- **Partial Fills**: Orders can be filled incrementally
- **Cancellations**: Orders can be canceled before completion
- **Real-time Updates**: All state changes are captured in real-time tables

## Use Cases

This example is useful for:

- Understanding order lifecycle management
- Building order monitoring dashboards
- Testing downstream systems that consume order data
- Learning Deephaven's table publishing and state management patterns

## Order States

Orders transition through the following states:

- **PendingSubmit**: Order created, waiting for submission to market
- **Submitted**: Order sent to market, awaiting execution
- **PartialFilled**: Order partially executed, remaining quantity still active
- **Filled**: Order completely executed
- **Canceled**: Order canceled before full execution

## State Transition Flow

```text
PendingSubmit → Submitted → PartialFilled → Filled
                    ↓
                PendingCancel → Canceled
```

## Running the Example

Copy the contents of [`simulated_orders.py`](./simulated_orders.py) and paste it into the Deephaven console. The simulation starts automatically and runs continuously, generating order state changes based on configurable probabilities.

### Simulation Parameters

The `SimulateOrders` class accepts parameters to control behavior:

- `syms`: List of symbols to trade (default: AAPL, GOOG, AMZN)
- `accounts`: List of trading accounts (default: ACCOUNT_1, ACCOUNT_2)
- `freq_sec_avg`: Average time between updates in seconds (default: 0.01)
- `prob_new_order`: Probability of creating a new order vs updating existing (default: 0.2)
- `prob_cancel`: Probability of canceling vs filling an order (default: 0.1)
- `order_sizes`: Possible order quantities (default: [100, 200, 300, 400, 500])

## Output Tables

- **`orders_blink`**: Blink table showing only the most recent state change (resets each update)
- **`orders_full`**: Append-only table containing complete history of all order state changes
- **`orders_last`**: Latest state for each order (one row per OrderId)
- **`orders_open`**: Currently active orders (excludes Filled and Canceled)

## Controlling the Simulation

Use the `SimulateOrders` instance methods:

- `so.stop()`: Stop the simulation
- `so.start()`: Start/restart the simulation

## Implementation Notes

The example demonstrates important Deephaven patterns:

- **Table Publisher**: Creates real-time data streams using `table_publisher`
- **Blink Tables**: Shows how to use blink tables for latest-value semantics
- **State Management**: Tracks object state and publishes changes to tables
- **Threading**: Safe concurrent updates using execution context
