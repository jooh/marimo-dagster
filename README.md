# marimo-dagster

[![CI](https://github.com/jooh/marimo-dagster/actions/workflows/ci.yml/badge.svg)](https://github.com/jooh/marimo-dagster/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
<!-- CHANGE ME: Uncomment after setting up codecov and add your token -->
<!-- [![codecov](https://codecov.io/gh/jooh/marimo-dagster/branch/main/graph/badge.svg?token=YOUR_CODECOV_TOKEN)](https://codecov.io/gh/jooh/marimo-dagster) -->
<!-- CHANGE ME: Uncomment after first PyPI publish -->
<!-- [![PyPI](https://img.shields.io/pypi/v/marimo-dagster.svg)](https://pypi.org/project/marimo-dagster/) -->

Bidirectional converter between [marimo](https://marimo.io) notebooks and
[dagster](https://dagster.io) asset modules. Explore data interactively in
marimo, then convert to dagster for production orchestration — or go the other
way to debug and iterate on existing pipelines.

## Features

- **Marimo to dagster**: `@app.cell` functions become `@dg.asset` functions,
  with the dependency graph inferred from cell inputs/outputs.
- **Dagster to marimo**: `@dg.asset` functions become `@app.cell` functions,
  with docstrings converted to markdown cells.
- **SQL cell conversion**: `mo.sql()` calls are rewritten to `duckdb.sql()` so
  SQL-based notebooks translate into runnable dagster assets.
- **PEP 723 metadata**: `# /// script` dependency blocks are preserved and
  framework dependencies are swapped automatically (marimo ↔ dagster).
- **Decorator kwargs**: `@dg.asset(group_name=..., compute_kind=...)` metadata
  survives the round trip.
- **Framework stripping**: dagster-specific parameters (`AssetExecutionContext`,
  resource types) and framework calls (`context.log.info()`) are removed during
  conversion, keeping the notebook focused on data logic.

## Installation

```bash
pip install marimo-dagster
```

Or with uv:

```bash
uv add marimo-dagster
```

## CLI

Convert a marimo notebook to a dagster asset module:

```bash
marimo-dagster to-dagster notebook.py assets.py
```

Convert a dagster asset module to a marimo notebook:

```bash
marimo-dagster to-marimo assets.py notebook.py
```

## Python API

```python
from marimo_dagster import marimo_to_dagster, dagster_to_marimo

dagster_source = marimo_to_dagster(open("notebook.py").read())
marimo_source = dagster_to_marimo(open("assets.py").read())
```

## Cell type handling

| Marimo cell type | Dagster conversion |
|---|---|
| CODE | `@dg.asset` function |
| SQL (`mo.sql()`) | `@dg.asset` using `duckdb.sql()` |
| IMPORT_ONLY | Merged into the dagster import block |
| MARKDOWN | Dropped (preserved as docstrings on round-trip) |
| UI (`mo.ui.*`) | Dropped (interactive-only) |
| DISPLAY_ONLY | Dropped (display-only) |

## Development

```bash
make test        # pytest with 100% coverage
make typecheck   # ty type checker
make ruff        # ruff linter
make all-tests   # all three
```

## Release process

Releases are automated on pushes to `main` by the CD workflow in `.github/workflows/cd.yml`.

1. The workflow determines the latest `v*` Git tag and runs `.github/workflows/cd_version.py` to
   resolve the next version. If the latest tag matches the current major/minor, it bumps the patch
   to one higher than the max of the current patch and the tag patch.
2. If `pyproject.toml` changes, the workflow commits the version bump back to `main`.
3. It tags the release as `v<version>`, builds the wheel with `uv build`, and creates a GitHub release.

To enable PyPI publishing, set `PUBLISH_TO_PYPI: true` in `.github/workflows/cd.yml` and configure
PyPI trusted publishing for this repository.
