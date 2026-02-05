"""Tests for marimo_dagster package and CLI."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

import marimo_dagster
from marimo_dagster.cli import app

runner = CliRunner()


def test_import() -> None:
    """Verify the package can be imported."""
    assert marimo_dagster.__doc__ is not None


class TestCLIHelp:
    """Tests for CLI help and structure."""

    def test_cli_help(self) -> None:
        """Verify CLI help works."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    def test_cli_has_to_dagster_command(self) -> None:
        """Verify CLI has to-dagster command."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "to-dagster" in result.stdout

    def test_cli_has_to_marimo_command(self) -> None:
        """Verify CLI has to-marimo command."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "to-marimo" in result.stdout


class TestCLIToDagster:
    """Tests for to-dagster CLI command."""

    def test_to_dagster_help(self) -> None:
        """Verify to-dagster help works."""
        result = runner.invoke(app, ["to-dagster", "--help"])
        assert result.exit_code == 0
        assert "marimo notebook" in result.stdout.lower()

    def test_to_dagster_missing_args(self) -> None:
        """Verify to-dagster fails without required arguments."""
        result = runner.invoke(app, ["to-dagster"])
        assert result.exit_code != 0

    def test_to_dagster_with_example(
        self, marimo_tier1_example: Path, tmp_path: Path
    ) -> None:
        """Test to-dagster command with tier 1 marimo examples."""
        output_file = tmp_path / "output.py"
        result = runner.invoke(
            app, ["to-dagster", str(marimo_tier1_example), str(output_file)]
        )
        # Should fail because converter raises NotImplementedError
        assert result.exit_code != 0
        assert "NotImplementedError" in result.stdout or "not yet implemented" in str(result.exception)

    def test_to_dagster_file_io(self, tmp_path: Path) -> None:
        """Test to-dagster file I/O with mocked converter."""
        input_file = tmp_path / "input.py"
        output_file = tmp_path / "output.py"
        input_file.write_text("# marimo source")

        with patch("marimo_dagster.cli.marimo_to_dagster", return_value="# dagster output"):
            result = runner.invoke(
                app, ["to-dagster", str(input_file), str(output_file)]
            )

        assert result.exit_code == 0
        assert output_file.exists()
        assert output_file.read_text() == "# dagster output"
        assert "Converted" in result.stdout


class TestCLIToMarimo:
    """Tests for to-marimo CLI command."""

    def test_to_marimo_help(self) -> None:
        """Verify to-marimo help works."""
        result = runner.invoke(app, ["to-marimo", "--help"])
        assert result.exit_code == 0
        assert "dagster" in result.stdout.lower()

    def test_to_marimo_missing_args(self) -> None:
        """Verify to-marimo fails without required arguments."""
        result = runner.invoke(app, ["to-marimo"])
        assert result.exit_code != 0

    def test_to_marimo_with_example(
        self, dagster_tier1_example: Path, tmp_path: Path
    ) -> None:
        """Test to-marimo command with tier 1 dagster examples."""
        output_file = tmp_path / "output.py"
        result = runner.invoke(
            app, ["to-marimo", str(dagster_tier1_example), str(output_file)]
        )
        # Should fail because converter raises NotImplementedError
        assert result.exit_code != 0
        assert "NotImplementedError" in result.stdout or "not yet implemented" in str(result.exception)

    def test_to_marimo_file_io(self, tmp_path: Path) -> None:
        """Test to-marimo file I/O with mocked converter."""
        input_file = tmp_path / "input.py"
        output_file = tmp_path / "output.py"
        input_file.write_text("# dagster source")

        with patch("marimo_dagster.cli.dagster_to_marimo", return_value="# marimo output"):
            result = runner.invoke(
                app, ["to-marimo", str(input_file), str(output_file)]
            )

        assert result.exit_code == 0
        assert output_file.exists()
        assert output_file.read_text() == "# marimo output"
        assert "Converted" in result.stdout
