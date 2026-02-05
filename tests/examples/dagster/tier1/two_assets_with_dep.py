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
def raw_data() -> None:
    """Upstream asset that produces raw data."""
    import json
    from pathlib import Path

    data = {"records": [{"id": 1, "value": 100}, {"id": 2, "value": 200}]}

    output_path = Path("data/raw_data.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset(deps=["raw_data"])
def processed_data() -> None:
    """Downstream asset that depends on raw_data."""
    import json
    from pathlib import Path

    input_path = Path("data/raw_data.json")
    raw = json.loads(input_path.read_text())

    # Simple transformation: calculate sum of values
    total = sum(record["value"] for record in raw["records"])
    result = {"total": total, "count": len(raw["records"])}

    output_path = Path("data/processed_data.json")
    output_path.write_text(json.dumps(result, indent=2))
