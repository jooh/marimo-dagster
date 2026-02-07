"""Bidirectional conversion between marimo notebooks and dagster assets."""

import ast

from marimo_dagster._dagster_ast import generate_dagster, parse_dagster
from marimo_dagster._ir import CellType, ImportItem, NotebookIR
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
    _convert_sql_cells(ir)
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


def _convert_sql_cells(ir: NotebookIR) -> None:
    """Convert SQL cells to CODE cells using duckdb.sql() instead of mo.sql()."""
    has_sql = any(c.cell_type == CellType.SQL for c in ir.cells)
    if not has_sql:
        return

    # Add unaliased duckdb import if not already present.
    # An aliased import (e.g., `import duckdb as db`) doesn't make `duckdb`
    # available as a bare name, so we need an unaliased import for duckdb.sql().
    has_unaliased_duckdb = any(
        imp.module == "duckdb" and imp.alias is None and imp.names is None
        for imp in ir.imports
    )
    if not has_unaliased_duckdb:
        ir.imports.append(ImportItem(module="duckdb"))

    # Ensure duckdb and polars are in the script dependencies so the
    # generated dagster module can actually run duckdb.sql(...).pl().
    _ensure_dependency(ir, "duckdb")
    _ensure_dependency(ir, "polars")

    for cell in ir.cells:
        if cell.cell_type == CellType.SQL:
            cell.cell_type = CellType.CODE
            cell.body_stmts = [_RewriteMoSql().visit(s) for s in cell.body_stmts]


def _ensure_dependency(ir: NotebookIR, package: str) -> None:
    """Add *package* to script metadata dependencies if not already present."""
    import re

    for dep in ir.metadata.dependencies:
        bare = re.split(r"[><=!~\[]", dep)[0].strip()
        if bare == package:
            return
    ir.metadata.dependencies.append(package)


class _RewriteMoSql(ast.NodeTransformer):
    """Rewrite mo.sql(...) calls to duckdb.sql(...).pl().

    mo.sql() returns a Polars DataFrame by default.  Plain duckdb.sql()
    returns a lazy DuckDBPyRelation, so we append .pl() to materialise
    the result as a Polars DataFrame and preserve the original semantics.
    """

    _MARIMO_SQL_KWARGS = {"output"}

    def visit_Call(self, node: ast.Call) -> ast.Call:
        self.generic_visit(node)
        if (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "sql"
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "mo"
        ):
            node.func.value.id = "duckdb"
            node.keywords = [
                kw for kw in node.keywords
                if kw.arg not in self._MARIMO_SQL_KWARGS
            ]
            # Wrap in .pl() so the result is a Polars DataFrame,
            # matching mo.sql()'s default return type.
            node = ast.Call(
                func=ast.Attribute(value=node, attr="pl", ctx=ast.Load()),
                args=[],
                keywords=[],
            )
        return node
