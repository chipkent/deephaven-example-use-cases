# A combined historical and live table

This directory provides an implementation for a combined historical and live table.  
The combined table can be used with either a Deephaven server or client.

Key features of the combined table include:
* Specific table method overrides are provided to keep historical and live tables separate as long as possible to maintain indexing.
* All other methods are passed through to the combined table.
* `where` has an additional argument that allows you to specify whether to immediately apply the filter to the historical or live table or defer the application.

If you would like to add further performance optimizations to `CombinedTable`, you just need to override
table methods in the `CombinedTable` class to handle the specific cases.

## Files

* [./combined_table_common.py](./combined_table_common.py) - The common code for the combined table.
* [./combined_table_server.py](./combined_table_server.py) - The server-side implementation of the combined table.
* [./combined_table_client.py](./combined_table_client.py) - The client-side implementation of the combined table.
* [./test_server.py](./test_server.py) - A test/example script for the server-side implementation.
* [./test_client.py](./test_client.py) - A test/example script for the client-side implementation.