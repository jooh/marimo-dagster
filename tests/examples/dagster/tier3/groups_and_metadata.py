"""Assets with groups, compute kinds, and rich metadata.

Demonstrates how to organize assets with groups and attach
metadata for better observability in the Dagster UI.
Inspired by the quickstart_etl example in the main Dagster repository.

Source: https://github.com/dagster-io/dagster
        examples/quickstart_etl/src/quickstart_etl/defs/assets.py
License: Apache-2.0
"""

import dagster as dg


@dg.asset(
    group_name="ingestion",
    compute_kind="API",
)
def api_data() -> dg.MaterializeResult:
    """Fetch data from an external API.

    This asset demonstrates:
    - group_name for organizing assets
    - compute_kind for showing the computation type
    - MaterializeResult for returning rich metadata
    """
    import json
    from pathlib import Path

    # Simulated API response
    data = {
        "records": [
            {"id": 1, "name": "Item A", "value": 100},
            {"id": 2, "name": "Item B", "value": 200},
            {"id": 3, "name": "Item C", "value": 150},
        ],
        "fetched_at": "2024-01-15T10:30:00Z",
    }

    output_path = Path("data/api_data.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))

    return dg.MaterializeResult(
        metadata={
            "num_records": len(data["records"]),
            "fetched_at": data["fetched_at"],
            "source": dg.MetadataValue.url("https://api.example.com/data"),
        }
    )


@dg.asset(
    deps=["api_data"],
    group_name="transformation",
    compute_kind="Pandas",
)
def transformed_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Transform the raw API data.

    Demonstrates logging and returning tabular metadata.
    """
    import json
    from pathlib import Path

    import pandas as pd

    input_path = Path("data/api_data.json")
    raw = json.loads(input_path.read_text())

    df = pd.DataFrame(raw["records"])
    df["value_normalized"] = df["value"] / df["value"].max()

    context.log.info(f"Transformed {len(df)} records")

    output_path = Path("data/transformed_data.csv")
    df.to_csv(output_path, index=False)

    return dg.MaterializeResult(
        metadata={
            "num_records": len(df),
            "columns": list(df.columns),
            "preview": dg.MetadataValue.md(df.head().to_markdown()),
        }
    )


@dg.asset(
    deps=["transformed_data"],
    group_name="reporting",
    compute_kind="Analysis",
    description="Generate summary statistics from transformed data.",
)
def summary_stats() -> dg.MaterializeResult:
    """Compute summary statistics.

    Demonstrates the description parameter and JSON metadata.
    """
    import json
    from pathlib import Path

    import pandas as pd

    df = pd.read_csv("data/transformed_data.csv")

    stats = {
        "total_value": float(df["value"].sum()),
        "mean_value": float(df["value"].mean()),
        "max_value": float(df["value"].max()),
        "min_value": float(df["value"].min()),
        "record_count": len(df),
    }

    output_path = Path("data/summary_stats.json")
    output_path.write_text(json.dumps(stats, indent=2))

    return dg.MaterializeResult(
        metadata={
            "stats": dg.MetadataValue.json(stats),
            "output_path": dg.MetadataValue.path(str(output_path)),
        }
    )


defs = dg.Definitions(
    assets=[api_data, transformed_data, summary_stats],
)
