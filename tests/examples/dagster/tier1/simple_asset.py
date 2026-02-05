"""Simple single asset example.

This is the most minimal Dagster asset definition possible.
Inspired by Dagster University Essentials Lesson 3.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_3/
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
def my_data() -> None:
    """A simple asset that writes static data to a file."""
    data = {"message": "Hello from Dagster!", "values": [1, 2, 3]}

    import json
    from pathlib import Path

    output_path = Path("data/my_data.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))
