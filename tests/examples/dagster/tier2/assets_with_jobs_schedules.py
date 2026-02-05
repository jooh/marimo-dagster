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
def daily_sales() -> dict:
    """Asset containing daily sales data."""
    return {
        "date": "2024-01-15",
        "total_sales": 15000.00,
        "transaction_count": 150,
    }


@dg.asset
def daily_inventory() -> dict:
    """Asset containing daily inventory snapshot."""
    return {
        "date": "2024-01-15",
        "total_items": 5000,
        "low_stock_items": 12,
    }


@dg.asset
def weekly_report(daily_sales: dict, daily_inventory: dict) -> dict:
    """Weekly summary report combining sales and inventory."""
    return {
        "report_type": "weekly",
        "sales_summary": daily_sales,
        "inventory_summary": daily_inventory,
    }


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
