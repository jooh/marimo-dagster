"""Simple single asset example using IOManager pattern.

This is the most minimal Dagster asset definition using the IOManager pattern
where the asset returns data directly instead of writing to files.
The IOManager (mem_io_manager for testing) handles persistence.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dagster>=1.9.0",
# ]
# ///

import dagster as dg


@dg.asset
def my_data() -> dict:
    """A simple asset that returns static data."""
    return {"message": "Hello from Dagster!", "values": [1, 2, 3]}


defs = dg.Definitions(
    assets=[my_data],
    resources={"io_manager": dg.mem_io_manager},
)
