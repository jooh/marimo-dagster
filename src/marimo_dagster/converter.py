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

    # Add duckdb import if not already present
    if not any(imp.module == "duckdb" for imp in ir.imports):
        ir.imports.append(ImportItem(module="duckdb"))

    for cell in ir.cells:
        if cell.cell_type == CellType.SQL:
            cell.cell_type = CellType.CODE
            cell.body_stmts = [_RewriteMoSql().visit(s) for s in cell.body_stmts]


class _RewriteMoSql(ast.NodeTransformer):
    """Rewrite mo.sql(...) calls to duckdb.sql(...), stripping marimo-specific kwargs."""

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
        return node
