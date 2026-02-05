"""Multiple assets registered via Definitions object.

Demonstrates the standard pattern for organizing assets using the Definitions API.
Inspired by Dagster University Essentials Lesson 5.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_5/
License: Apache-2.0
"""

import dagster as dg


@dg.asset
def users() -> list[dict]:
    """Asset representing user data."""
    return [
        {"id": 1, "name": "Alice", "active": True},
        {"id": 2, "name": "Bob", "active": True},
        {"id": 3, "name": "Charlie", "active": False},
    ]


@dg.asset
def products() -> list[dict]:
    """Asset representing product catalog."""
    return [
        {"sku": "A001", "name": "Widget", "price": 9.99},
        {"sku": "A002", "name": "Gadget", "price": 19.99},
    ]


@dg.asset
def user_product_summary(users: list[dict], products: list[dict]) -> dict:
    """Asset that combines users and products data."""
    return {
        "active_users": sum(1 for u in users if u["active"]),
        "total_products": len(products),
        "total_catalog_value": sum(p["price"] for p in products),
    }


defs = dg.Definitions(
    assets=[users, products, user_product_summary],
)
