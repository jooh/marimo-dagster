"""Two assets with a dependency relationship.

Demonstrates the basic pattern of one asset depending on another.
Inspired by Dagster University Essentials Lesson 4.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_4/
License: Apache-2.0
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
    """Downstream asset that depends on raw_data."""
    total = sum(record["value"] for record in raw_data["records"])
    return {"total": total, "count": len(raw_data["records"])}
