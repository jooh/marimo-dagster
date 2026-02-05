"""Three assets in a linear chain (A -> B -> C) using IOManager pattern.

Demonstrates multi-hop dependency traversal where data flows through
function parameters and return values.
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "dagster>=1.9.0",
# ]
# ///

import dagster as dg


@dg.asset
def source_data() -> dict:
    """First asset in the chain - produces source data."""
    return {"items": ["apple", "banana", "cherry", "date"]}


@dg.asset
def filtered_data(source_data: dict) -> dict:
    """Second asset - filters the source data."""
    # Filter to items with more than 5 characters
    return {"items": [item for item in source_data["items"] if len(item) > 5]}


@dg.asset
def final_report(filtered_data: dict) -> dict:
    """Third asset - produces final report from filtered data."""
    return {
        "summary": f"Found {len(filtered_data['items'])} items",
        "items": filtered_data["items"],
    }
