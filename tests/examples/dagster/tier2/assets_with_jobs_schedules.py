"""Assets with jobs and schedules.

Demonstrates how to define jobs to materialize subsets of assets
and schedules to run them on a cron schedule.
Inspired by Dagster University Essentials Lesson 7.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_7/
License: Apache-2.0
"""

import dagster as dg


@dg.asset
def daily_sales() -> None:
    """Asset containing daily sales data."""
    import json
    from pathlib import Path

    data = {
        "date": "2024-01-15",
        "total_sales": 15000.00,
        "transaction_count": 150,
    }

    output_path = Path("data/daily_sales.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset
def daily_inventory() -> None:
    """Asset containing daily inventory snapshot."""
    import json
    from pathlib import Path

    data = {
        "date": "2024-01-15",
        "total_items": 5000,
        "low_stock_items": 12,
    }

    output_path = Path("data/daily_inventory.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset(deps=["daily_sales", "daily_inventory"])
def weekly_report() -> None:
    """Weekly summary report combining sales and inventory."""
    import json
    from pathlib import Path

    sales = json.loads(Path("data/daily_sales.json").read_text())
    inventory = json.loads(Path("data/daily_inventory.json").read_text())

    report = {
        "report_type": "weekly",
        "sales_summary": sales,
        "inventory_summary": inventory,
    }

    output_path = Path("data/weekly_report.json")
    output_path.write_text(json.dumps(report, indent=2))


# Define asset selections for jobs
daily_assets = dg.AssetSelection.assets("daily_sales", "daily_inventory")
weekly_assets = dg.AssetSelection.assets("weekly_report")

# Define jobs
daily_update_job = dg.define_asset_job(
    name="daily_update_job",
    selection=daily_assets,
)

weekly_update_job = dg.define_asset_job(
    name="weekly_update_job",
    selection=weekly_assets,
)

# Define schedules
daily_schedule = dg.ScheduleDefinition(
    job=daily_update_job,
    cron_schedule="0 0 * * *",  # Every day at midnight
)

weekly_schedule = dg.ScheduleDefinition(
    job=weekly_update_job,
    cron_schedule="0 0 * * 1",  # Every Monday at midnight
)

defs = dg.Definitions(
    assets=[daily_sales, daily_inventory, weekly_report],
    jobs=[daily_update_job, weekly_update_job],
    schedules=[daily_schedule, weekly_schedule],
)
