"""Pytest fixtures for marimo-dagster tests."""

import os
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).parent / "examples"
MARIMO_TIER1_DIR = EXAMPLES_DIR / "marimo" / "tier1"
DAGSTER_TIER1_DIR = EXAMPLES_DIR / "dagster" / "tier1"
DAGSTER_TIER2_DIR = EXAMPLES_DIR / "dagster" / "tier2"


def _get_example_files(directory: Path) -> list[Path]:
    """Get all Python example files from a directory."""
    if not directory.exists():
        return []
    return sorted(
        p for p in directory.glob("*.py") if p.name != "__init__.py"
    )


# Example files by tier
MARIMO_TIER1_EXAMPLES = _get_example_files(MARIMO_TIER1_DIR)
DAGSTER_TIER1_EXAMPLES = _get_example_files(DAGSTER_TIER1_DIR)
DAGSTER_TIER2_EXAMPLES = _get_example_files(DAGSTER_TIER2_DIR)


@pytest.fixture(params=MARIMO_TIER1_EXAMPLES, ids=lambda p: p.stem)
def marimo_tier1_example(request: pytest.FixtureRequest) -> Path:
    """Parametrized fixture yielding each tier 1 marimo example."""
    return request.param


@pytest.fixture(params=DAGSTER_TIER1_EXAMPLES, ids=lambda p: p.stem)
def dagster_tier1_example(request: pytest.FixtureRequest) -> Path:
    """Parametrized fixture yielding each tier 1 dagster example."""
    return request.param


@pytest.fixture(params=DAGSTER_TIER2_EXAMPLES, ids=lambda p: p.stem)
def dagster_tier2_example(request: pytest.FixtureRequest) -> Path:
    """Parametrized fixture yielding each tier 2 dagster example."""
    return request.param


@pytest.fixture
def example_venv_runner(tmp_path: Path) -> Callable[[Path, str], subprocess.CompletedProcess[str]]:
    """Create a factory for running code in isolated venvs with example deps.

    This fixture creates per-example venvs using UV with dependencies extracted
    from PEP 723 script metadata.

    Usage:
        def test_something(example_venv_runner, marimo_tier1_example):
            result = example_venv_runner(marimo_tier1_example, "python -c 'print(1)'")
    """
    venv_cache: dict[Path, Path] = {}

    def _runner(example_path: Path, command: str) -> subprocess.CompletedProcess[str]:
        # Create venv for this example if not already cached
        if example_path not in venv_cache:
            venv_path = tmp_path / f"venv_{example_path.stem}"
            # Create venv with uv
            subprocess.run(
                ["uv", "venv", str(venv_path)],
                check=True,
                capture_output=True,
            )
            # Install deps from PEP 723 metadata using uv pip
            subprocess.run(
                ["uv", "pip", "install", "--python", str(venv_path / "bin" / "python"), "-r", str(example_path)],
                check=True,
                capture_output=True,
            )
            venv_cache[example_path] = venv_path

        venv_path = venv_cache[example_path]

        # Run command in the venv
        return subprocess.run(
            command,
            shell=True,
            env={"PATH": f"{venv_path / 'bin'}:{os.environ.get('PATH', '')}"},
            capture_output=True,
            text=True,
        )

    return _runner
