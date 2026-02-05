"""Placeholder tests for marimo_dagster."""

import marimo_dagster

from typer.testing import CliRunner

from marimo_dagster.cli import app

runner = CliRunner()


def test_import() -> None:
    """Verify the package can be imported."""
    assert marimo_dagster.__doc__ is not None


def test_cli_hello() -> None:
    """Verify CLI hello command works."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "hello" in result.stdout.lower()


def test_cli_hello_default() -> None:
    """Verify CLI hello command with default name."""
    result = runner.invoke(app, [])
    assert result.exit_code == 0
    assert "Hello, world!" in result.stdout


def test_cli_hello_with_name() -> None:
    """Verify CLI hello command with name option."""
    result = runner.invoke(app, ["--name", "World"])
    assert result.exit_code == 0
    assert "Hello, World!" in result.stdout

