"""Two assets with a dependency relationship using IOManager pattern.

Demonstrates the basic pattern of one asset depending on another,
where data flows through function parameters and return values.
The IOManager handles loading the upstream asset's output.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dagster>=1.9.0",
# ]
# ///

import dagster as dg


@dg.asset
def raw_data() -> dict:
    """Upstream asset that produces raw data."""
    return {"records": [{"id": 1, "value": 100}, {"id": 2, "value": 200}]}


@dg.asset
def processed_data(raw_data: dict) -> dict:
    """Downstream asset that depends on raw_data via function parameter."""
    total = sum(record["value"] for record in raw_data["records"])
    return {"total": total, "count": len(raw_data["records"])}


defs = dg.Definitions(
    assets=[raw_data, processed_data],
    resources={"io_manager": dg.mem_io_manager},
)
