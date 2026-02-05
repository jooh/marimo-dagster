"""Three assets in a linear chain (A -> B -> C).

Demonstrates multi-hop dependency traversal.
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
def source_data() -> None:
    """First asset in the chain - produces source data."""
    import json
    from pathlib import Path

    data = {"items": ["apple", "banana", "cherry", "date"]}

    output_path = Path("data/source_data.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset(deps=["source_data"])
def filtered_data() -> None:
    """Second asset - filters the source data."""
    import json
    from pathlib import Path

    input_path = Path("data/source_data.json")
    source = json.loads(input_path.read_text())

    # Filter to items with more than 5 characters
    filtered = {"items": [item for item in source["items"] if len(item) > 5]}

    output_path = Path("data/filtered_data.json")
    output_path.write_text(json.dumps(filtered, indent=2))


@dg.asset(deps=["filtered_data"])
def final_report() -> None:
    """Third asset - produces final report from filtered data."""
    import json
    from pathlib import Path

    input_path = Path("data/filtered_data.json")
    filtered = json.loads(input_path.read_text())

    report = {
        "summary": f"Found {len(filtered['items'])} items",
        "items": filtered["items"],
    }

    output_path = Path("data/final_report.json")
    output_path.write_text(json.dumps(report, indent=2))
