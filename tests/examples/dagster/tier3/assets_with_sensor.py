"""Assets triggered by sensors.

Demonstrates how to use sensors to trigger asset materialization
based on external events like new files appearing.
Inspired by Dagster University Essentials Lesson 9.

Source: https://github.com/dagster-io/project-dagster-university
        dagster_university/dagster_essentials/src/dagster_essentials/completed/lesson_9/
License: Apache-2.0
"""

import json
import os

import dagster as dg


@dg.asset
def adhoc_request(context: dg.AssetExecutionContext) -> None:
    """Process an ad-hoc data request.

    This asset is triggered by the sensor when new request files appear.
    The request configuration is passed via run config.
    """
    from pathlib import Path

    # Get config from run config (would be populated by sensor)
    config = context.op_execution_context.op_config
    filename = config.get("filename", "unknown")
    request_type = config.get("request_type", "default")
    parameters = config.get("parameters", {})

    context.log.info(f"Processing request from file: {filename}")
    context.log.info(f"Request type: {request_type}")

    result = {
        "source_file": filename,
        "request_type": request_type,
        "parameters": parameters,
        "status": "completed",
    }

    output_path = Path(f"data/adhoc_results/{filename}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, indent=2))


# Define a job for the sensor to trigger
adhoc_request_job = dg.define_asset_job(
    name="adhoc_request_job",
    selection=dg.AssetSelection.assets("adhoc_request"),
    config={
        "ops": {
            "adhoc_request": {
                "config": {
                    "filename": str,
                    "request_type": str,
                    "parameters": dict,
                }
            }
        }
    },
)


@dg.sensor(job=adhoc_request_job, minimum_interval_seconds=30)
def adhoc_request_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    """Sensor that watches for new request files.

    Monitors a directory for new JSON request files and triggers
    the adhoc_request_job for each new or modified file.
    """
    requests_dir = os.environ.get("REQUESTS_DIR", "data/requests")

    # Load previous state from cursor
    previous_state = json.loads(context.cursor) if context.cursor else {}
    current_state = {}
    runs_to_request = []

    # Check if directory exists
    if not os.path.isdir(requests_dir):
        return dg.SensorResult(
            run_requests=[],
            cursor=json.dumps(current_state),
        )

    # Scan for request files
    for filename in os.listdir(requests_dir):
        if not filename.endswith(".json"):
            continue

        file_path = os.path.join(requests_dir, filename)
        if not os.path.isfile(file_path):
            continue

        last_modified = os.path.getmtime(file_path)
        current_state[filename] = last_modified

        # Check if file is new or modified
        if filename not in previous_state or previous_state[filename] != last_modified:
            with open(file_path) as f:
                request_config = json.load(f)

            runs_to_request.append(
                dg.RunRequest(
                    run_key=f"adhoc_request_{filename}_{last_modified}",
                    run_config={
                        "ops": {
                            "adhoc_request": {
                                "config": {
                                    "filename": filename,
                                    **request_config,
                                }
                            }
                        }
                    },
                )
            )

    return dg.SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(current_state),
    )


defs = dg.Definitions(
    assets=[adhoc_request],
    jobs=[adhoc_request_job],
    sensors=[adhoc_request_sensor],
)
