# marimo-dagster

[![CI](https://github.com/jooh/marimo-dagster/actions/workflows/ci.yml/badge.svg)](https://github.com/jooh/marimo-dagster/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
<!-- CHANGE ME: Uncomment after setting up codecov and add your token -->
<!-- [![codecov](https://codecov.io/gh/jooh/marimo-dagster/branch/main/graph/badge.svg?token=YOUR_CODECOV_TOKEN)](https://codecov.io/gh/jooh/marimo-dagster) -->
<!-- CHANGE ME: Uncomment after first PyPI publish -->
<!-- [![PyPI](https://img.shields.io/pypi/v/marimo-dagster.svg)](https://pypi.org/project/marimo-dagster/) -->

Take Marimo notebooks to production as Dagster assets

## Installation

```bash
pip install marimo-dagster
```

Or with uv:

```bash
uv add marimo-dagster
```

## CLI

```bash
marimo-dagster --help
```

## Library

```python
import marimo_dagster
```

## Development

```bash
make test
make typecheck
make ruff
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
