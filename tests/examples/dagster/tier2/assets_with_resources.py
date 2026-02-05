"""Assets that use resources for external dependencies.

Demonstrates the resource injection pattern for database connections.
Inspired by Dagster University Essentials Lessons 6-7.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_6/
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_7/
License: Apache-2.0
"""

import dagster as dg
from dagster import ConfigurableResource


class DatabaseResource(ConfigurableResource):
    """A simple configurable database resource.

    In real usage, this would be something like DuckDBResource or
    a connection pool to PostgreSQL, etc.
    """

    connection_string: str

    def query(self, sql: str) -> list[dict]:
        """Execute a query and return results."""
        # Simulated query execution
        return [{"result": f"Executed: {sql}"}]

    def execute(self, sql: str) -> None:
        """Execute a statement."""
        pass


@dg.asset
def raw_events(database: DatabaseResource) -> None:
    """Load raw events using the database resource."""
    import json
    from pathlib import Path

    # In a real scenario, this would use the database resource
    # to load data from a source table
    _ = database.query("SELECT * FROM source_events LIMIT 100")

    # Simulated data
    events = [
        {"event_id": 1, "type": "click", "timestamp": "2024-01-01T10:00:00"},
        {"event_id": 2, "type": "view", "timestamp": "2024-01-01T10:01:00"},
        {"event_id": 3, "type": "click", "timestamp": "2024-01-01T10:02:00"},
    ]

    output_path = Path("data/raw_events.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(events, indent=2))


@dg.asset(deps=["raw_events"])
def event_counts(database: DatabaseResource) -> None:
    """Aggregate event counts using the database resource."""
    import json
    from pathlib import Path

    events = json.loads(Path("data/raw_events.json").read_text())

    # Count events by type
    counts: dict[str, int] = {}
    for event in events:
        event_type = event["type"]
        counts[event_type] = counts.get(event_type, 0) + 1

    # In real usage, might write this back to database
    database.execute("INSERT INTO event_counts VALUES (...)")

    output_path = Path("data/event_counts.json")
    output_path.write_text(json.dumps(counts, indent=2))


# Resource configuration
database_resource = DatabaseResource(
    connection_string=dg.EnvVar("DATABASE_URL"),
)

defs = dg.Definitions(
    assets=[raw_events, event_counts],
    resources={"database": database_resource},
)
