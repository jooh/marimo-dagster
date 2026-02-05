"""CLI for marimo-dagster."""

from pathlib import Path

import typer

from marimo_dagster.converter import dagster_to_marimo, marimo_to_dagster

app = typer.Typer()


@app.command()
def to_dagster(
    input_file: Path = typer.Argument(..., help="Path to marimo notebook"),
    output_file: Path = typer.Argument(..., help="Path for output dagster module"),
) -> None:
    """Convert a marimo notebook to a dagster asset module."""
    source = input_file.read_text()
    result = marimo_to_dagster(source)
    output_file.write_text(result)
    print(f"Converted {input_file} -> {output_file}")


@app.command()
def to_marimo(
    input_file: Path = typer.Argument(..., help="Path to dagster asset module"),
    output_file: Path = typer.Argument(..., help="Path for output marimo notebook"),
) -> None:
    """Convert a dagster asset module to a marimo notebook."""
    source = input_file.read_text()
    result = dagster_to_marimo(source)
    output_file.write_text(result)
    print(f"Converted {input_file} -> {output_file}")
