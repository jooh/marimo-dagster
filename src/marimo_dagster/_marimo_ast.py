"""Marimo AST parsing and generation."""

import ast
import textwrap
from importlib.metadata import PackageNotFoundError, version

from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR
from marimo_dagster._metadata import generate_pep723_metadata, parse_pep723_metadata


def _marimo_version() -> str:
    """Return the installed marimo version, or a fallback."""
    try:
        return version("marimo")
    except PackageNotFoundError:
        return "0.0.0"

_MARIMO_MODULES = {"marimo"}
_FRAMEWORK_PARAMS = {"mo"}


def parse_marimo(source: str) -> NotebookIR:
    """Parse a marimo notebook into the intermediate representation."""
    tree = ast.parse(source)
    metadata = parse_pep723_metadata(source)
    module_docstring = ast.get_docstring(tree)

    imports: list[ImportItem] = []
    cells: list[CellNode] = []

    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and _is_app_cell(node):
            _process_cell(node, imports, cells)

    return NotebookIR(
        imports=imports,
        cells=cells,
        metadata=metadata,
        module_docstring=module_docstring,
    )


def _is_app_cell(node: ast.FunctionDef) -> bool:
    """Check if a function is decorated with @app.cell or @app.cell(...)."""
    for dec in node.decorator_list:
        if isinstance(dec, ast.Attribute) and dec.attr == "cell":
            if isinstance(dec.value, ast.Name) and dec.value.id == "app":
                return True
        if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Attribute):
            if dec.func.attr == "cell" and isinstance(dec.func.value, ast.Name):
                if dec.func.value.id == "app":
                    return True
    return False


def _process_cell(
    node: ast.FunctionDef,
    imports: list[ImportItem],
    cells: list[CellNode],
) -> None:
    """Classify a cell and add it to imports or cells as appropriate."""
    params = [arg.arg for arg in node.args.args]
    data_params = [p for p in params if p not in _FRAMEWORK_PARAMS]

    # Extract body (all statements) and return info
    body_stmts = list(node.body)
    outputs = _extract_outputs(body_stmts)

    # Strip the return statement from body
    if body_stmts and isinstance(body_stmts[-1], ast.Return):
        body_stmts = body_stmts[:-1]

    cell_type = _classify_cell(body_stmts, params, outputs)

    if cell_type == CellType.IMPORT_ONLY:
        # Extract imports into the IR imports list (filtering framework).
        # All stmts are guaranteed Import or ImportFrom by classification.
        for stmt in body_stmts:
            if isinstance(stmt, ast.Import):
                for alias in stmt.names:
                    if alias.name not in _MARIMO_MODULES:
                        imports.append(ImportItem(module=alias.name, alias=alias.asname))
            if isinstance(stmt, ast.ImportFrom):
                if stmt.module and not any(
                    stmt.module == m or stmt.module.startswith(m + ".")
                    for m in _MARIMO_MODULES
                ):
                    names = [(a.name, a.asname) for a in stmt.names]
                    imports.append(ImportItem(module=stmt.module, names=names))
        return

    # Determine cell name from outputs
    name = _infer_name(outputs, len(cells))

    cells.append(CellNode(
        name=name,
        body_stmts=body_stmts,
        inputs=data_params,
        outputs=outputs,
        cell_type=cell_type,
    ))


def _extract_outputs(body_stmts: list[ast.stmt]) -> list[str]:
    """Extract output variable names from the cell's return statement."""
    if not body_stmts:
        return []
    last = body_stmts[-1]
    if not isinstance(last, ast.Return) or last.value is None:
        return []

    ret_val = last.value
    # return (x,) or return (x, y)
    if isinstance(ret_val, ast.Tuple):
        names = []
        for elt in ret_val.elts:
            if isinstance(elt, ast.Name):
                names.append(elt.id)
        return names
    # return x
    if isinstance(ret_val, ast.Name):
        return [ret_val.id]
    return []


def _classify_cell(
    body_stmts: list[ast.stmt],
    params: list[str],
    outputs: list[str],
) -> CellType:
    """Classify a cell based on its body content."""
    if not body_stmts:
        return CellType.CODE

    # Import-only: all statements are imports (possibly with trivial assignments)
    all_imports = all(
        isinstance(s, (ast.Import, ast.ImportFrom)) for s in body_stmts
    )
    if all_imports:
        return CellType.IMPORT_ONLY

    # Display-only: single bare name expression, no outputs
    if (
        len(body_stmts) == 1
        and isinstance(body_stmts[0], ast.Expr)
        and isinstance(body_stmts[0].value, ast.Name)
        and not outputs
    ):
        return CellType.DISPLAY_ONLY

    # Check for mo.* calls
    has_mo_sql = _has_call_pattern(body_stmts, "mo", "sql")
    has_mo_ui = _has_mo_ui_or_widget(body_stmts)

    # Markdown: single statement is a top-level mo.md() call
    if len(body_stmts) == 1 and not outputs and _is_top_level_mo_call(body_stmts[0], "md"):
        return CellType.MARKDOWN

    # SQL: contains mo.sql()
    if has_mo_sql:
        return CellType.SQL

    # UI: contains mo.accordion, mo.ui.*, etc. with no data outputs
    if has_mo_ui and not outputs:
        return CellType.UI

    return CellType.CODE


def _is_top_level_mo_call(stmt: ast.stmt, method: str) -> bool:
    """Check if a statement is a top-level mo.method() call (not nested)."""
    if not isinstance(stmt, ast.Expr):
        return False
    call = stmt.value
    if not isinstance(call, ast.Call):
        return False
    return (
        isinstance(call.func, ast.Attribute)
        and call.func.attr == method
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "mo"
    )


def _has_call_pattern(stmts: list[ast.stmt], obj: str, method: str) -> bool:
    """Check if any statement contains a call to obj.method()."""
    for stmt in stmts:
        for node in ast.walk(stmt):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == method
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == obj
            ):
                return True
    return False


def _has_mo_ui_or_widget(stmts: list[ast.stmt]) -> bool:
    """Check if any statement contains mo.ui.*, mo.accordion, etc."""
    _widget_methods = {"accordion", "hstack", "vstack", "tabs", "icon"}
    for stmt in stmts:
        for node in ast.walk(stmt):
            if isinstance(node, ast.Attribute):
                # mo.ui.*
                if (
                    isinstance(node.value, ast.Attribute)
                    and node.value.attr == "ui"
                    and isinstance(node.value.value, ast.Name)
                    and node.value.value.id == "mo"
                ):
                    return True
                # mo.accordion, mo.hstack, etc.
                if (
                    node.attr in _widget_methods
                    and isinstance(node.value, ast.Name)
                    and node.value.id == "mo"
                ):
                    return True
    return False


def _infer_name(outputs: list[str], cell_index: int) -> str:
    """Infer a cell name from its outputs or generate one."""
    # Use first non-underscore-prefixed output
    for name in outputs:
        if not name.startswith("_"):
            return name
    return f"cell_{cell_index}"


def generate_marimo(ir: NotebookIR) -> str:
    """Generate a marimo notebook from the intermediate representation."""
    sections: list[str] = []

    # Module docstring
    if ir.module_docstring:
        sections.append(f'"""{ir.module_docstring}"""')

    # PEP 723 metadata
    meta_block = generate_pep723_metadata(ir.metadata)
    if meta_block:
        sections.append(meta_block.rstrip("\n"))

    # Marimo preamble
    sections.append("import marimo")
    sections.append(
        f'__generated_with = "{_marimo_version()}"\n'
        f'app = marimo.App(width="medium")'
    )

    # Import cell (always generated — at minimum provides `mo`)
    sections.append(_generate_import_cell(ir.imports))

    # Code cells (with optional markdown cell for docstrings)
    for cell in ir.cells:
        if cell.docstring:
            sections.append(_generate_markdown_cell(cell.docstring))
        sections.append(_generate_code_cell(cell))

    # Main guard
    sections.append('if __name__ == "__main__":\n    app.run()')

    return "\n\n\n".join(sections) + "\n"


def _generate_import_cell(imports: list[ImportItem]) -> str:
    """Generate the import cell containing marimo and all non-framework imports."""
    lines: list[str] = []
    lines.append("@app.cell")
    lines.append("def _():")
    lines.append("    import marimo as mo")

    return_names: list[str] = ["mo"]

    for imp in imports:
        if imp.names is not None:
            # from X import a, b
            name_parts = []
            for name, alias in imp.names:
                if alias:
                    name_parts.append(f"{name} as {alias}")
                    return_names.append(alias)
                else:
                    name_parts.append(name)
                    return_names.append(name)
            lines.append(f"    from {imp.module} import {', '.join(name_parts)}")
        else:
            # import X [as alias]
            if imp.alias:
                lines.append(f"    import {imp.module} as {imp.alias}")
                return_names.append(imp.alias)
            else:
                lines.append(f"    import {imp.module}")
                return_names.append(imp.module)

    lines.append(f"    return ({', '.join(return_names)})")
    return "\n".join(lines)


def _generate_markdown_cell(text: str) -> str:
    """Generate a markdown cell for a docstring."""
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return (
        "@app.cell(hide_code=True)\n"
        "def _(mo):\n"
        f'    mo.md(r"""\n'
        f"{textwrap.indent(escaped, '    ')}\n"
        f'    """)\n'
        "    return"
    )


def _generate_code_cell(cell: "CellNode") -> str:  # noqa: F821
    """Generate a code cell from a CellNode."""
    from marimo_dagster._ir import CellNode  # avoid circular at module level

    assert isinstance(cell, CellNode)

    # Build parameter list
    params = ", ".join(cell.inputs) if cell.inputs else ""

    lines: list[str] = []
    lines.append("@app.cell")
    lines.append(f"def _({params}):")

    # Unparse each body statement
    for stmt in cell.body_stmts:
        stmt_text = ast.unparse(stmt)
        for line in stmt_text.splitlines():
            lines.append(f"    {line}")

    # Return statement
    if cell.outputs:
        outputs_str = ", ".join(cell.outputs)
        # Trailing comma for tuple
        if len(cell.outputs) == 1:
            lines.append(f"    return ({outputs_str},)")
        else:
            lines.append(f"    return ({outputs_str})")
    else:
        lines.append("    return")

    return "\n".join(lines)
