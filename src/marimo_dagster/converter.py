"""Bidirectional conversion between marimo notebooks and dagster assets."""

from marimo_dagster._dagster_ast import generate_dagster, parse_dagster
from marimo_dagster._marimo_ast import generate_marimo, parse_marimo
from marimo_dagster._metadata import transform_dependencies


def marimo_to_dagster(source: str) -> str:
    """Convert marimo notebook source to dagster asset module.

    Args:
        source: The source code of a marimo notebook.

    Returns:
        The source code of a dagster asset module.
    """
    ir = parse_marimo(source)
    ir.metadata.dependencies = transform_dependencies(
        ir.metadata.dependencies,
        from_framework="marimo",
        to_framework="dagster",
    )
    return generate_dagster(ir)


def dagster_to_marimo(source: str) -> str:
    """Convert dagster asset module source to marimo notebook.

    Args:
        source: The source code of a dagster asset module.

    Returns:
        The source code of a marimo notebook.
    """
    ir = parse_dagster(source)
    ir.metadata.dependencies = transform_dependencies(
        ir.metadata.dependencies,
        from_framework="dagster",
        to_framework="marimo",
    )
    return generate_marimo(ir)
