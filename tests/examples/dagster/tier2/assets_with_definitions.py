"""Multiple assets registered via Definitions object.

Demonstrates the standard pattern for organizing assets using the Definitions API.
Inspired by Dagster University Essentials Lesson 5.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_5/
License: Apache-2.0
"""

import dagster as dg


@dg.asset
def users() -> None:
    """Asset representing user data."""
    import json
    from pathlib import Path

    data = [
        {"id": 1, "name": "Alice", "active": True},
        {"id": 2, "name": "Bob", "active": True},
        {"id": 3, "name": "Charlie", "active": False},
    ]

    output_path = Path("data/users.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset
def products() -> None:
    """Asset representing product catalog."""
    import json
    from pathlib import Path

    data = [
        {"sku": "A001", "name": "Widget", "price": 9.99},
        {"sku": "A002", "name": "Gadget", "price": 19.99},
    ]

    output_path = Path("data/products.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(data, indent=2))


@dg.asset(deps=["users", "products"])
def user_product_summary() -> None:
    """Asset that combines users and products data."""
    import json
    from pathlib import Path

    users_data = json.loads(Path("data/users.json").read_text())
    products_data = json.loads(Path("data/products.json").read_text())

    summary = {
        "active_users": sum(1 for u in users_data if u["active"]),
        "total_products": len(products_data),
        "total_catalog_value": sum(p["price"] for p in products_data),
    }

    output_path = Path("data/user_product_summary.json")
    output_path.write_text(json.dumps(summary, indent=2))


defs = dg.Definitions(
    assets=[users, products, user_product_summary],
)
