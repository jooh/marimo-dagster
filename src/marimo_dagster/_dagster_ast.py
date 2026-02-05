"""Dagster AST parsing and generation."""

import ast

from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR
from marimo_dagster._metadata import generate_pep723_metadata, parse_pep723_metadata

_DAGSTER_MODULES = {"dagster"}


def parse_dagster(source: str) -> NotebookIR:
    """Parse a dagster asset module into the intermediate representation."""
    tree = ast.parse(source)
    metadata = parse_pep723_metadata(source)
    module_docstring = ast.get_docstring(tree)

    imports: list[ImportItem] = []
    cells: list[CellNode] = []
    # Track local names that resolve to dagster's `asset` decorator
    # (e.g., `from dagster import asset` or `from dagster import asset as my_asset`)
    asset_names: set[str] = set()

    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in _DAGSTER_MODULES:
                    imports.append(ImportItem(module=alias.name, alias=alias.asname))
        elif isinstance(node, ast.ImportFrom):
            if node.module and any(
                node.module == m or node.module.startswith(m + ".")
                for m in _DAGSTER_MODULES
            ):
                # Collect local names that are aliases for `asset`
                for alias in node.names:
                    if alias.name == "asset":
                        asset_names.add(alias.asname or alias.name)
            elif node.module:
                names = [(a.name, a.asname) for a in node.names]
                imports.append(ImportItem(module=node.module, names=names))
        elif isinstance(node, ast.FunctionDef) and _is_dagster_asset(
            node, asset_names=asset_names
        ):
            cells.append(_parse_asset_function(node))

    return NotebookIR(
        imports=imports,
        cells=cells,
        metadata=metadata,
        module_docstring=module_docstring,
    )


def generate_dagster(ir: NotebookIR) -> str:
    """Generate a dagster asset module from the intermediate representation."""
    sections: list[str] = []

    # Module docstring
    if ir.module_docstring:
        sections.append(f'"""{ir.module_docstring}"""')

    # PEP 723 metadata
    meta_block = generate_pep723_metadata(ir.metadata)
    if meta_block:
        sections.append(meta_block.rstrip("\n"))

    # Imports
    import_lines = ["import dagster as dg"]
    for imp in ir.imports:
        if imp.names is not None:
            name_parts = []
            for name, alias in imp.names:
                if alias:
                    name_parts.append(f"{name} as {alias}")
                else:
                    name_parts.append(name)
            import_lines.append(f"from {imp.module} import {', '.join(name_parts)}")
        elif imp.alias:
            import_lines.append(f"import {imp.module} as {imp.alias}")
        else:
            import_lines.append(f"import {imp.module}")
    sections.append("\n".join(import_lines))

    # Asset functions (only CODE cells)
    for cell in ir.cells:
        if cell.cell_type != CellType.CODE:
            continue
        sections.append(_generate_asset_function(cell))

    return "\n\n\n".join(sections) + "\n"


def _generate_asset_function(cell: CellNode) -> str:
    """Generate a single @dg.asset function from a CellNode."""
    lines: list[str] = []

    # Decorator
    lines.append("@dg.asset")

    # Function signature
    params = ", ".join(cell.inputs)
    ret_type = f" -> {cell.return_type_annotation}" if cell.return_type_annotation else ""
    lines.append(f"def {cell.name}({params}){ret_type}:")

    # Docstring
    if cell.docstring:
        lines.append(f'    """{cell.docstring}"""')

    # Body statements
    for stmt in cell.body_stmts:
        stmt_text = ast.unparse(stmt)
        for line in stmt_text.splitlines():
            lines.append(f"    {line}")

    # Return: the last body statement should be an assignment `name = ...`
    # Convert it to a return statement
    if cell.outputs:
        lines.append(f"    return {cell.outputs[0]}")

    return "\n".join(lines)


def _is_dagster_asset(
    node: ast.FunctionDef, *, asset_names: set[str] | None = None
) -> bool:
    """Check if a function is decorated with a dagster asset decorator.

    Supports attribute forms (@dg.asset, @dagster.asset) and bare name forms
    (@asset) when the name is in asset_names (tracked from `from dagster import asset`).
    """
    _asset_names = asset_names or set()
    for dec in node.decorator_list:
        # @dg.asset / @dagster.asset
        if isinstance(dec, ast.Attribute) and dec.attr == "asset":
            if isinstance(dec.value, ast.Name) and dec.value.id in ("dg", "dagster"):
                return True
        # @dg.asset(...) / @dagster.asset(...)
        if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
            if dec.func.attr == "asset" and isinstance(dec.func.value, ast.Name):
                if dec.func.value.id in ("dg", "dagster"):
                    return True
        # @asset (bare name from `from dagster import asset`)
        if isinstance(dec, ast.Name) and dec.id in _asset_names:
            return True
        # @asset(...) (bare call from `from dagster import asset`)
        if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name):
            if dec.func.id in _asset_names:
                return True
    return False


def _parse_asset_function(node: ast.FunctionDef) -> CellNode:
    """Extract a CellNode from a dagster asset function definition."""
    name = node.name
    docstring = ast.get_docstring(node)

    # Extract inputs (parameter names), filtering framework params
    inputs = [
        arg.arg
        for arg in node.args.args
        if not _is_framework_param(arg)
    ]

    # Extract return type annotation
    return_type = ast.unparse(node.returns) if node.returns else None

    # Build body: strip docstring and final return, transform return to assignment
    body_stmts = _transform_body(node, name, has_docstring=docstring is not None)

    return CellNode(
        name=name,
        body_stmts=body_stmts,
        inputs=inputs,
        outputs=[name],
        cell_type=CellType.CODE,
        docstring=docstring,
        return_type_annotation=return_type,
    )


def _is_framework_param(arg: ast.arg) -> bool:
    """Check if a function parameter is a dagster framework param (e.g. context)."""
    if arg.annotation:
        ann = ast.unparse(arg.annotation)
        if "AssetExecutionContext" in ann:
            return True
    return False


def _transform_body(
    node: ast.FunctionDef, asset_name: str, *, has_docstring: bool
) -> list[ast.stmt]:
    """Transform dagster function body for the IR.

    - Strips the docstring (first Expr(Constant(str)))
    - Converts final ``return expr`` to ``asset_name = expr``
    """
    stmts = list(node.body)

    # Strip docstring
    if has_docstring and stmts:
        stmts = stmts[1:]

    if not stmts:
        return stmts

    # Transform final return into assignment
    last = stmts[-1]
    if isinstance(last, ast.Return) and last.value is not None:
        assign = ast.Assign(
            targets=[ast.Name(id=asset_name, ctx=ast.Store())],
            value=last.value,
            lineno=last.lineno,
            col_offset=last.col_offset,
        )
        stmts[-1] = assign

    return stmts
