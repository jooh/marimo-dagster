"""Diamond-shaped dependency graph (A -> B, A -> C, B -> D, C -> D).

Demonstrates complex dependency resolution where multiple paths
converge on a single downstream asset.
Inspired by patterns in Dagster University Essentials Lesson 4.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_4/
License: Apache-2.0

Dependency Graph:
       source
       /    \
      v      v
   left    right
      \    /
       v  v
      merged
"""

import dagster as dg


@dg.asset
def source() -> dict:
    """Root asset that feeds into two downstream assets."""
    return {
        "values": [10, 20, 30, 40, 50],
        "metadata": {"created_by": "source"},
    }


@dg.asset
def left_branch(source: dict) -> dict:
    """Left branch - computes sum of values."""
    return {
        "operation": "sum",
        "result": sum(source["values"]),
    }


@dg.asset
def right_branch(source: dict) -> dict:
    """Right branch - computes average of values."""
    values = source["values"]
    return {
        "operation": "average",
        "result": sum(values) / len(values) if values else 0,
    }


@dg.asset
def merged(left_branch: dict, right_branch: dict) -> dict:
    """Merged asset - combines results from both branches."""
    return {
        "combined": {
            "sum": left_branch["result"],
            "average": right_branch["result"],
        },
        "sources": ["left_branch", "right_branch"],
    }


defs = dg.Definitions(
    assets=[source, left_branch, right_branch, merged],
)
