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
def my_data() -> dict:
    """A simple asset that produces static data."""
    return {"message": "Hello from Dagster!", "values": [1, 2, 3]}
