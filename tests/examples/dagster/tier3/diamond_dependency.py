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
def source() -> None:
    """Root asset that feeds into two downstream assets."""
    import json
    from pathlib import Path

    data = {
        "values": [10, 20, 30, 40, 50],
        "metadata": {"created_by": "source"},
    }

    output_path = Path("data/diamond/source.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset(deps=["source"])
def left_branch() -> None:
    """Left branch - computes sum of values."""
    import json
    from pathlib import Path

    source_data = json.loads(Path("data/diamond/source.json").read_text())

    result = {
        "operation": "sum",
        "result": sum(source_data["values"]),
    }

    output_path = Path("data/diamond/left_branch.json")
    output_path.write_text(json.dumps(result, indent=2))


@dg.asset(deps=["source"])
def right_branch() -> None:
    """Right branch - computes average of values."""
    import json
    from pathlib import Path

    source_data = json.loads(Path("data/diamond/source.json").read_text())
    values = source_data["values"]

    result = {
        "operation": "average",
        "result": sum(values) / len(values) if values else 0,
    }

    output_path = Path("data/diamond/right_branch.json")
    output_path.write_text(json.dumps(result, indent=2))


@dg.asset(deps=["left_branch", "right_branch"])
def merged() -> None:
    """Merged asset - combines results from both branches."""
    import json
    from pathlib import Path

    left = json.loads(Path("data/diamond/left_branch.json").read_text())
    right = json.loads(Path("data/diamond/right_branch.json").read_text())

    result = {
        "combined": {
            "sum": left["result"],
            "average": right["result"],
        },
        "sources": ["left_branch", "right_branch"],
    }

    output_path = Path("data/diamond/merged.json")
    output_path.write_text(json.dumps(result, indent=2))


defs = dg.Definitions(
    assets=[source, left_branch, right_branch, merged],
)
