"""Bidirectional conversion between marimo notebooks and dagster assets."""

from marimo_dagster._dagster_ast import generate_dagster, parse_dagster
from marimo_dagster._marimo_ast import generate_marimo, parse_marimo


def marimo_to_dagster(source: str) -> str:
    """Convert marimo notebook source to dagster asset module.

    Args:
        source: The source code of a marimo notebook.

    Returns:
        The source code of a dagster asset module.
    """
    ir = parse_marimo(source)
    return generate_dagster(ir)


def dagster_to_marimo(source: str) -> str:
    """Convert dagster asset module source to marimo notebook.

    Args:
        source: The source code of a dagster asset module.

    Returns:
        The source code of a marimo notebook.
    """
    ir = parse_dagster(source)
    return generate_marimo(ir)
