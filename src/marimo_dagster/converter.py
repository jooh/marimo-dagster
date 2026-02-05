"""Bidirectional conversion between marimo notebooks and dagster assets."""

import ast
import re
from dataclasses import dataclass


@dataclass
class AssetInfo:
    """Information about a dagster asset extracted from AST."""

    name: str
    deps: list[str]
    body: list[ast.stmt]
    return_expr: ast.expr | None
    docstring: str | None


def _extract_pep723_metadata(source: str) -> str | None:
    """Extract PEP 723 script metadata block from source."""
    pattern = r"^# /// script\n((?:#.*\n)*)# ///$"
    match = re.search(pattern, source, re.MULTILINE)
    if match:
        return match.group(0)
    return None


def _transform_pep723_metadata(metadata: str | None) -> str | None:
    """Transform PEP 723 metadata from dagster to marimo deps."""
    if metadata is None:
        return None

    # Replace dagster dependency with marimo
    # Pattern matches lines like: #     "dagster>=1.9.0",
    result = re.sub(
        r'^(#\s+)"dagster[^"]*"',
        r'\1"marimo"',
        metadata,
        flags=re.MULTILINE,
    )

    return result


def _is_asset_decorator(decorator: ast.expr) -> bool:
    """Check if a decorator is an asset decorator.

    Handles both import styles:
    - import dagster as dg; @dg.asset / @dg.asset(...)
    - from dagster import asset; @asset / @asset(...)
    """
    # @dg.asset (Attribute)
    if isinstance(decorator, ast.Attribute):
        return decorator.attr == "asset"
    # @asset (Name - direct import)
    elif isinstance(decorator, ast.Name):
        return decorator.id == "asset"
    # @dg.asset(...) or @asset(...) (Call)
    elif isinstance(decorator, ast.Call):
        func = decorator.func
        if isinstance(func, ast.Attribute):
            return func.attr == "asset"
        elif isinstance(func, ast.Name):
            return func.id == "asset"
    return False


def _extract_assets(tree: ast.Module) -> list[AssetInfo]:
    """Extract asset information from AST."""
    assets = []

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Check if it has @dg.asset decorator
            is_asset = any(_is_asset_decorator(d) for d in node.decorator_list)
            if not is_asset:
                continue

            # Extract function name (asset name)
            name = node.name

            # Extract dependencies from function parameters (skip 'self')
            deps = [arg.arg for arg in node.args.args if arg.arg != "self"]

            # Extract docstring
            docstring = ast.get_docstring(node)

            # Extract body and return expression
            body = list(node.body)
            return_expr = None

            # If there's a docstring, it's the first statement - skip it
            if docstring and body and isinstance(body[0], ast.Expr):
                body = body[1:]

            # Find and extract return statement
            if body and isinstance(body[-1], ast.Return):
                return_stmt: ast.Return = body.pop()  # type: ignore[assignment]
                return_expr = return_stmt.value

            assets.append(
                AssetInfo(
                    name=name,
                    deps=deps,
                    body=body,
                    return_expr=return_expr,
                    docstring=docstring,
                )
            )

    return assets


def _generate_cell(asset: AssetInfo) -> str:
    """Generate a marimo cell from an asset."""
    # Build parameter list
    params = ", ".join(asset.deps) if asset.deps else ""

    # Build body
    body_lines = []

    # Add asset marker comment
    body_lines.append(f"    # asset: {asset.name}")

    # Add docstring as comment if present
    if asset.docstring:
        for line in asset.docstring.split("\n"):
            body_lines.append(f"    # {line}" if line else "    #")

    # Add body statements
    for stmt in asset.body:
        stmt_code = ast.unparse(stmt)
        for line in stmt_code.split("\n"):
            body_lines.append(f"    {line}")

    # Add assignment and return
    if asset.return_expr is not None:
        return_code = ast.unparse(asset.return_expr)
        body_lines.append(f"    {asset.name} = {return_code}")
    else:
        body_lines.append(f"    {asset.name} = None")

    body_lines.append(f"    return ({asset.name},)")

    # Combine into cell
    lines = [
        "@app.cell",
        f"def _({params}):",
        *body_lines,
    ]

    return "\n".join(lines)


def _topological_sort(assets: list[AssetInfo]) -> list[AssetInfo]:
    """Sort assets in dependency order (dependencies first)."""
    # Build name -> asset mapping
    by_name = {a.name: a for a in assets}

    # Build adjacency list (asset -> assets that depend on it)
    visited: set[str] = set()
    result: list[AssetInfo] = []

    def visit(name: str) -> None:
        if name in visited:
            return
        visited.add(name)
        asset = by_name.get(name)
        if asset:
            for dep in asset.deps:
                visit(dep)
            result.append(asset)

    for asset in assets:
        visit(asset.name)

    return result


def dagster_to_marimo(source: str) -> str:
    """Convert dagster asset module source to marimo notebook.

    Args:
        source: The source code of a dagster asset module.

    Returns:
        The source code of a marimo notebook.
    """
    # Parse the source
    tree = ast.parse(source)

    # Extract PEP 723 metadata
    metadata = _extract_pep723_metadata(source)
    transformed_metadata = _transform_pep723_metadata(metadata)

    # Extract assets
    assets = _extract_assets(tree)

    # Sort by dependencies
    sorted_assets = _topological_sort(assets)

    # Generate marimo notebook
    parts = []

    # Add transformed PEP 723 metadata if present
    if transformed_metadata:
        parts.append(transformed_metadata)
        parts.append("")

    # Add marimo boilerplate
    parts.append("import marimo")
    parts.append("")
    parts.append("app = marimo.App()")
    parts.append("")

    # Generate cells for each asset
    for asset in sorted_assets:
        parts.append("")
        parts.append(_generate_cell(asset))

    # Add main block
    parts.append("")
    parts.append("")
    parts.append('if __name__ == "__main__":')
    parts.append("    app.run()")
    parts.append("")

    return "\n".join(parts)


def marimo_to_dagster(source: str) -> str:
    """Convert marimo notebook source to dagster asset module.

    Args:
        source: The source code of a marimo notebook.

    Returns:
        The source code of a dagster asset module.
    """
    raise NotImplementedError("marimo_to_dagster conversion not yet implemented")
