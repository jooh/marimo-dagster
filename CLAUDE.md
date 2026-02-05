# Agent guide for marimo-dagster

## Repository overview
- `src/marimo_dagster/` contains the library and CLI implementation.
- `tests/` holds pytest coverage for core behavior.
- `pyproject.toml` defines dependencies, entry points, and tooling.

## Development setup
- Requires Python 3.12+ (see `pyproject.toml`).
- Environment management is with uv.
- Run Python and related CLI tools via `uv run` so they use the uv virtualenv.

## Common commands
- Run tests: `make test`
- Run type checks: `make typecheck`
- Run ruff checks: `make ruff`
- Run all checks: `make all-tests`

## Style and conventions
- TDD for all code development - write test, then run to verify it fails, then develop, then verify the test passes.
- All tasks should end by running `make all-tests` and verifying it passes.
- Prefer updating or adding pytest tests in `tests/` for behavior changes.
- For CLI changes, update both `src/marimo_dagster/cli.py` and any relevant tests.
- Target modern Python 3.12+ syntax, no need to be backwards compatible.

## Tips
- The main package is `marimo_dagster`.
- The CLI entry point is `marimo_dagster.cli:app`.
