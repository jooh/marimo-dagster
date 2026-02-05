"""Assets with groups, compute kinds, and rich metadata.

Demonstrates how to organize assets with groups and attach
metadata for better observability in the Dagster UI.
Inspired by the quickstart_etl example in the main Dagster repository.

Source: https://github.com/dagster-io/dagster
        examples/quickstart_etl/src/quickstart_etl/defs/assets.py
License: Apache-2.0
"""

import dagster as dg
import pandas as pd


@dg.asset(
    group_name="ingestion",
    compute_kind="API",
)
def api_data(context: dg.AssetExecutionContext) -> dict:
    """Fetch data from an external API.

    This asset demonstrates:
    - group_name for organizing assets
    - compute_kind for showing the computation type
    - context.add_output_metadata for attaching rich metadata
    """
    data = {
        "records": [
            {"id": 1, "name": "Item A", "value": 100},
            {"id": 2, "name": "Item B", "value": 200},
            {"id": 3, "name": "Item C", "value": 150},
        ],
        "fetched_at": "2024-01-15T10:30:00Z",
    }

    context.add_output_metadata({
        "num_records": len(data["records"]),
        "fetched_at": data["fetched_at"],
        "source": dg.MetadataValue.url("https://api.example.com/data"),
    })

    return data


@dg.asset(
    group_name="transformation",
    compute_kind="Pandas",
)
def transformed_data(
    context: dg.AssetExecutionContext, api_data: dict
) -> pd.DataFrame:
    """Transform the raw API data.

    Demonstrates logging and returning tabular metadata.
    """
    df = pd.DataFrame(api_data["records"])
    df["value_normalized"] = df["value"] / df["value"].max()

    context.log.info(f"Transformed {len(df)} records")

    context.add_output_metadata({
        "num_records": len(df),
        "columns": list(df.columns),
        "preview": dg.MetadataValue.md(df.head().to_markdown()),
    })

    return df


@dg.asset(
    group_name="reporting",
    compute_kind="Analysis",
    description="Generate summary statistics from transformed data.",
)
def summary_stats(context: dg.AssetExecutionContext, transformed_data: pd.DataFrame) -> dict:
    """Compute summary statistics.

    Demonstrates the description parameter and JSON metadata.
    """
    stats = {
        "total_value": float(transformed_data["value"].sum()),
        "mean_value": float(transformed_data["value"].mean()),
        "max_value": float(transformed_data["value"].max()),
        "min_value": float(transformed_data["value"].min()),
        "record_count": len(transformed_data),
    }

    context.add_output_metadata({
        "stats": dg.MetadataValue.json(stats),
    })

    return stats


defs = dg.Definitions(
    assets=[api_data, transformed_data, summary_stats],
)
