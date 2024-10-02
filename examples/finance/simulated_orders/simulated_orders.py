""" This example simulates orders from an order management system. 

Possible order states are:
* PendingSubmit - waiting for submission
* Submitted - submitted but not filled
* PartialFilled - submitted and partially filled
* Filled - fully filled
* Canceled - canceled before being fully filled
"""

from typing import Sequence
from datetime import datetime
from dataclasses import dataclass
import asyncio, random, threading, time

from deephaven import new_table, dtypes as dht
from deephaven.column import string_col, double_col, long_col, datetime_col
from deephaven.stream import blink_to_append_only
from deephaven.stream.table_publisher import table_publisher
from deephaven.execution_context import get_exec_ctx


class SimulateOrders:
    """ Simulated order management system. """

    @dataclass
    class Order:
        """ State of an order. """
        id: int
        sym: str
        account: str
        state: str
        desired: int
        filled: int

    def __init__(self, 
            syms: Sequence[str], 
            accounts: Sequence[str], 
            freq_sec_avg: float=1e-2, 
            freq_sec_std: float=1e-2, 
            prob_new_order: float=0.2, 
            prob_cancel: float=0.1, 
            order_sizes: Sequence[int]=[100, 200, 300, 400, 500]
        ):
        """ Creates an order.
        
        Args:
            syms: symbols to simulate
            accounts: accounts to simulate
            freq_sec_avg: average frequency, in seconds, for updating the state
            freq_sec_std: standard deviation, in seconds, of the frequency for updating the state.
            prob_new_order: probability of creating a new order vs updating an old order
            prob_cancel: probability of canceling vs filling an order
            order_sizes: possible order sizes
        """

        self.freq_sec_avg = freq_sec_avg
        self.freq_sec_std = freq_sec_std
        self.syms = syms
        self.accounts = accounts
        self.prob_new_order = prob_new_order
        self.prob_cancel = prob_cancel
        self.order_sizes = order_sizes

        coldefs = {
            "Timestamp": dht.Instant,
            "OrderId": dht.int64,
            "Sym": dht.string,
            "Account": dht.string,
            "State": dht.string,
            "Desired": dht.int64,
            "Filled": dht.int64,
        }

        def shut_down():
            print("Shutting down SimulateOrders.")

        self.blink_table, self._publisher = table_publisher(
            name="SimulateOrders", col_defs=coldefs, on_shutdown_callback=shut_down
        )

        self.full_table = blink_to_append_only(self.blink_table)

        self._nextId = 0
        self._orders = []


    def stop(self):
        """ Stop the simulation. """
        self._publisher.publish_failure(RuntimeError("Stopping SimulateOrders"))

    def start(self):
        """ Start the simulatiion. """
        ctx = get_exec_ctx()

        def thread_func():
            with ctx:
                while True:
                    self._update()
                    time.sleep(abs(random.gauss(mu=self.freq_sec_avg, sigma=self.freq_sec_std)))

        thread = threading.Thread(target=thread_func)
        thread.start()

    def _update(self):
        if self._orders and random.random() < 1-self.prob_new_order:
            order = random.choice(self._orders)

            if order.state == "PendingSubmit":
                order.state = "Submitted"
            elif order.state == "PendingCancel":
                order.state = "Canceled"
            elif order.state in ["Submitted", "PartialFilled"]:
                if random.random() < self.prob_cancel:
                    order.state = "PendingCancel"
                else:
                    order.filled = min(order.filled + random.choice(self.order_sizes), order.desired)
                    order.state = "PartialFilled" if order.filled < order.desired else "Filled"
        else:
            self._nextId += 1
            order = self.Order(
                id = self._nextId,
                sym = random.choice(self.syms),
                account = random.choice(self.accounts),
                state = "PendingSubmit",
                desired = random.choice(self.order_sizes),
                filled = 0,
            )
            self._orders.append(order)

        if order.state in ["Filled", "Canceled"]:
            self._orders.remove(order)

        t = new_table([
            datetime_col("Timestamp", [datetime.now()]),
            long_col("OrderId", [order.id]),
            string_col("Sym", [order.sym]),
            string_col("Account", [order.account]),
            string_col("State", [order.state]),
            long_col("Desired", [order.desired]),
            long_col("Filled", [order.filled]),
        ])

        self._publisher.add(t)


syms = ["AAPL", "GOOG", "AMZN"]
accounts = ["ACCOUNT_1", "ACCOUNT_2"]
so = SimulateOrders(syms=syms, accounts=accounts)
so.start()

orders_blink = so.blink_table
orders_full = so.full_table
orders_last = orders_full.last_by("OrderId")
orders_open = orders_last.where("State not in `Filled`, `Canceled`")



