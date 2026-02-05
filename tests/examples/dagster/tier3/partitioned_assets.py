"""Assets with partition definitions.

Demonstrates how to define partitioned assets for processing data
in time-based or categorical slices.
Inspired by Dagster University Essentials Lesson 8.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_8/
License: Apache-2.0
"""

import dagster as dg


# Define partition schemes
monthly_partition = dg.MonthlyPartitionsDefinition(
    start_date="2024-01-01",
    end_date="2024-12-31",
)

weekly_partition = dg.WeeklyPartitionsDefinition(
    start_date="2024-01-01",
    end_date="2024-03-31",
)


@dg.asset(partitions_def=monthly_partition)
def monthly_sales_data(context: dg.AssetExecutionContext) -> dict:
    """Monthly sales data, partitioned by month."""
    partition_key = context.partition_key
    context.log.info(f"Processing partition: {partition_key}")

    return {
        "month": partition_key,
        "total_sales": 50000.00,
        "orders": 500,
    }


@dg.asset(partitions_def=weekly_partition)
def weekly_metrics(context: dg.AssetExecutionContext) -> dict:
    """Weekly metrics, partitioned by week."""
    partition_key = context.partition_key
    context.log.info(f"Processing partition: {partition_key}")

    return {
        "week_start": partition_key,
        "active_users": 1200,
        "page_views": 45000,
    }


@dg.asset(partitions_def=monthly_partition)
def monthly_sales_report(
    context: dg.AssetExecutionContext, monthly_sales_data: dict
) -> dict:
    """Monthly report derived from partitioned sales data."""
    partition_key = context.partition_key

    return {
        "month": partition_key,
        "summary": f"Total sales: ${monthly_sales_data['total_sales']:,.2f}",
        "avg_order_value": monthly_sales_data["total_sales"]
        / monthly_sales_data["orders"],
    }


# Schedule that runs for the previous month's partition
monthly_sales_job = dg.define_asset_job(
    name="monthly_sales_job",
    selection=dg.AssetSelection.assets("monthly_sales_data", "monthly_sales_report"),
    partitions_def=monthly_partition,
)

monthly_sales_schedule = dg.build_schedule_from_partitions(
    job=monthly_sales_job,
    name="monthly_sales_schedule",
)

defs = dg.Definitions(
    assets=[monthly_sales_data, weekly_metrics, monthly_sales_report],
    jobs=[monthly_sales_job],
    schedules=[monthly_sales_schedule],
)
