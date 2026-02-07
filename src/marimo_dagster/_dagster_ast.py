"""Parse dagster asset modules into IR and generate dagster source from IR."""

import ast

from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR
from marimo_dagster._metadata import generate_pep723_metadata, parse_pep723_metadata

_DAGSTER_MODULES = {"dagster"}
_DAGSTER_RESOURCE_BASES = {"ConfigurableResource", "ConfigurableIOManager"}


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
    # Track resource base class names imported from dagster
    resource_bases: set[str] = set()
    # Track locally-defined resource class names
    resource_types: set[str] = set()

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
                    if alias.name in _DAGSTER_RESOURCE_BASES:
                        resource_bases.add(alias.asname or alias.name)
            elif node.module:
                names = [(a.name, a.asname) for a in node.names]
                imports.append(ImportItem(module=node.module, names=names))
        elif isinstance(node, ast.ClassDef):
            # Track classes inheriting from dagster resource bases
            for base in node.bases:
                if isinstance(base, ast.Name) and base.id in resource_bases:
                    resource_types.add(node.name)
                if isinstance(base, ast.Attribute) and base.attr in _DAGSTER_RESOURCE_BASES:
                    if isinstance(base.value, ast.Name) and base.value.id in ("dg", "dagster"):
                        resource_types.add(node.name)
        elif isinstance(node, ast.FunctionDef) and _is_dagster_multi_asset(node):
            cells.append(
                _parse_multi_asset_function(
                    node, resource_types=resource_types
                )
            )
        elif isinstance(node, ast.FunctionDef) and _is_dagster_asset(
            node, asset_names=asset_names
        ):
            cells.append(
                _parse_asset_function(
                    node, resource_types=resource_types, asset_names=asset_names
                )
            )

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
        import_lines.append(imp.format_statement())
    sections.append("\n".join(import_lines))

    # Asset functions (only CODE cells)
    for cell in ir.cells:
        if cell.cell_type != CellType.CODE:
            continue
        sections.append(_generate_asset_function(cell))

    return "\n\n\n".join(sections) + "\n"


def _generate_asset_function(cell: CellNode) -> str:
    """Generate a single @dg.asset or @dg.multi_asset function from a CellNode."""
    if len(cell.outputs) > 1:
        return _generate_multi_asset_function(cell)

    lines: list[str] = []

    # Decorator
    if cell.decorator_kwargs:
        kwargs_parts = [f'{k}="{v}"' for k, v in cell.decorator_kwargs.items()]
        lines.append(f"@dg.asset({', '.join(kwargs_parts)})")
    else:
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


def _generate_multi_asset_function(cell: CellNode) -> str:
    """Generate a @dg.multi_asset function from a CellNode with multiple outputs."""
    lines: list[str] = []

    # Build decorator kwargs
    kwargs_parts: list[str] = []

    # outs dict
    outs_entries = [f'        "{name}": dg.AssetOut()' for name in cell.outputs]
    outs_str = "{\n" + ",\n".join(outs_entries) + ",\n    }"
    kwargs_parts.append(f"outs={outs_str}")

    # Additional decorator kwargs
    for k, v in cell.decorator_kwargs.items():
        kwargs_parts.append(f'{k}="{v}"')

    decorator_args = ",\n    ".join(kwargs_parts)
    lines.append(f"@dg.multi_asset(\n    {decorator_args},\n)")

    # Function signature
    params = ", ".join(cell.inputs)
    lines.append(f"def {cell.name}({params}):")

    # Docstring
    if cell.docstring:
        lines.append(f'    """{cell.docstring}"""')

    # Body statements
    for stmt in cell.body_stmts:
        stmt_text = ast.unparse(stmt)
        for line in stmt_text.splitlines():
            lines.append(f"    {line}")

    # Return all outputs as tuple
    lines.append(f"    return {', '.join(cell.outputs)}")

    return "\n".join(lines)


def _is_dagster_asset(
    node: ast.FunctionDef, *, asset_names: set[str] | None = None
) -> bool:
    """Check if a function is decorated with a dagster asset decorator.

    Supports attribute forms (@dg.asset, @dagster.asset), call forms
    (@dg.asset(...)), and bare name forms (@asset) when the name is in
    asset_names (tracked from ``from dagster import asset``).
    """
    _asset_names = asset_names or set()
    for dec in node.decorator_list:
        # @dg.asset / @dagster.asset (non-call attribute form)
        if isinstance(dec, ast.Attribute) and dec.attr == "asset":
            if isinstance(dec.value, ast.Name) and dec.value.id in ("dg", "dagster"):
                return True
        # @asset (bare name from `from dagster import asset`)
        if isinstance(dec, ast.Name) and dec.id in _asset_names:
            return True
        # Call forms: @dg.asset(...), @dagster.asset(...), @asset(...)
        if isinstance(dec, ast.Call) and _is_asset_decorator_call(dec, asset_names=_asset_names):
            return True
    return False


def _parse_asset_function(
    node: ast.FunctionDef,
    *,
    resource_types: set[str] | None = None,
    asset_names: set[str] | None = None,
) -> CellNode:
    """Extract a CellNode from a dagster asset function definition."""
    name = node.name
    docstring = ast.get_docstring(node)

    _resource_types = resource_types or set()

    # Extract inputs and track stripped (framework/resource) param names
    inputs: list[str] = []
    stripped_params: set[str] = set()
    for arg in node.args.args:
        if _is_framework_param(arg, resource_types=_resource_types):
            stripped_params.add(arg.arg)
        else:
            inputs.append(arg.arg)

    # Extract return type annotation
    return_type = ast.unparse(node.returns) if node.returns else None

    # Extract decorator kwargs (string-literal values only)
    decorator_kwargs = _extract_decorator_kwargs(node, asset_names=asset_names)

    # Use description as docstring if function has no docstring
    if not docstring and "description" in decorator_kwargs:
        docstring = decorator_kwargs["description"]

    # Build body: strip docstring and final return, transform return to assignment
    body_stmts = _transform_body(node, name, has_docstring=ast.get_docstring(node) is not None)

    # Strip calls to stripped params (context.log.info, database.execute, etc.)
    if stripped_params:
        body_stmts = _strip_framework_calls(body_stmts, stripped_params)

    return CellNode(
        name=name,
        body_stmts=body_stmts,
        inputs=inputs,
        outputs=[name],
        cell_type=CellType.CODE,
        docstring=docstring,
        return_type_annotation=return_type,
        decorator_kwargs=decorator_kwargs,
    )


def _is_dagster_multi_asset(node: ast.FunctionDef) -> bool:
    """Check if a function is decorated with @dg.multi_asset(...) or @dagster.multi_asset(...)."""
    for dec in node.decorator_list:
        if not isinstance(dec, ast.Call):
            continue
        func = dec.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr == "multi_asset"
            and isinstance(func.value, ast.Name)
            and func.value.id in ("dg", "dagster")
        ):
            return True
    return False


def _parse_multi_asset_function(
    node: ast.FunctionDef,
    *,
    resource_types: set[str] | None = None,
) -> CellNode:
    """Extract a CellNode from a dagster multi_asset function definition."""
    name = node.name
    docstring = ast.get_docstring(node)

    _resource_types = resource_types or set()

    # Extract inputs, filtering framework/resource params
    inputs: list[str] = []
    stripped_params: set[str] = set()
    for arg in node.args.args:
        if _is_framework_param(arg, resource_types=_resource_types):
            stripped_params.add(arg.arg)
        else:
            inputs.append(arg.arg)

    # Extract outputs from outs={...} in decorator
    outputs = _extract_multi_asset_outs(node)

    # Build body: strip docstring and final return (no assignment transform)
    body_stmts = _strip_body(node, has_docstring=docstring is not None)

    # Strip calls to stripped params
    if stripped_params:
        body_stmts = _strip_framework_calls(body_stmts, stripped_params)

    return CellNode(
        name=name,
        body_stmts=body_stmts,
        inputs=inputs,
        outputs=outputs,
        cell_type=CellType.CODE,
        docstring=docstring,
    )


def _extract_multi_asset_outs(node: ast.FunctionDef) -> list[str]:
    """Extract output names from the outs={...} kwarg of @dg.multi_asset(...)."""
    for dec in node.decorator_list:
        if not isinstance(dec, ast.Call):
            continue
        func = dec.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr == "multi_asset"
            and isinstance(func.value, ast.Name)
            and func.value.id in ("dg", "dagster")
        ):
            continue
        for kw in dec.keywords:
            if kw.arg == "outs" and isinstance(kw.value, ast.Dict):
                names: list[str] = []
                for key in kw.value.keys:
                    if isinstance(key, ast.Constant) and isinstance(key.value, str):
                        names.append(key.value)
                return names
    return []


def _strip_body(
    node: ast.FunctionDef, *, has_docstring: bool
) -> list[ast.stmt]:
    """Strip docstring and final return from a function body."""
    stmts = list(node.body)

    # Strip docstring
    if has_docstring and stmts:
        stmts = stmts[1:]

    if not stmts:
        return stmts

    # Strip final return (don't transform to assignment for multi_asset)
    last = stmts[-1]
    if isinstance(last, ast.Return):
        stmts = stmts[:-1]

    return stmts


def _is_framework_param(
    arg: ast.arg, *, resource_types: set[str] | None = None
) -> bool:
    """Check if a function parameter is a dagster framework param.

    Matches AssetExecutionContext and any locally-defined resource types.
    """
    if arg.annotation:
        ann = ast.unparse(arg.annotation)
        if "AssetExecutionContext" in ann:
            return True
        if resource_types:
            for rt in resource_types:
                if rt in ann:
                    return True
    return False


def _extract_decorator_kwargs(
    node: ast.FunctionDef, *, asset_names: set[str] | None = None
) -> dict[str, str]:
    """Extract string-literal keyword arguments from the @dg.asset(...) decorator.

    Only extracts from decorators that are identified as dagster asset decorators,
    ignoring kwargs from other decorators.
    """
    _asset_names = asset_names or set()
    for dec in node.decorator_list:
        if not isinstance(dec, ast.Call):
            continue
        if not _is_asset_decorator_call(dec, asset_names=_asset_names):
            continue
        kwargs: dict[str, str] = {}
        for kw in dec.keywords:
            if kw.arg and isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                kwargs[kw.arg] = kw.value.value
        return kwargs
    return {}


def _is_asset_decorator_call(dec: ast.Call, *, asset_names: set[str] | None = None) -> bool:
    """Check if a Call decorator node is a dagster asset decorator call."""
    _asset_names = asset_names or set()
    # @dg.asset(...) / @dagster.asset(...)
    if isinstance(dec.func, ast.Attribute):
        if dec.func.attr == "asset" and isinstance(dec.func.value, ast.Name):
            if dec.func.value.id in ("dg", "dagster"):
                return True
    # @asset(...) (bare call from `from dagster import asset`)
    if isinstance(dec.func, ast.Name) and dec.func.id in _asset_names:
        return True
    return False


def _call_chain_root(node: ast.expr) -> str | None:
    """Get the root variable name of an attribute/call chain (e.g. context.log.info → 'context')."""
    while isinstance(node, ast.Attribute):
        node = node.value
    if isinstance(node, ast.Name):
        return node.id
    return None


def _strip_framework_calls(
    stmts: list[ast.stmt], stripped_names: set[str]
) -> list[ast.stmt]:
    """Remove statements that are calls on stripped framework variables.

    Strips:
    - Bare expression calls: ``context.log.info(...)``, ``database.execute(...)``
    - Discarded assignments: ``_ = database.query(...)``
    """
    result: list[ast.stmt] = []
    for stmt in stmts:
        # Bare expression call: context.log.info(...)
        if (
            isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Call)
            and _call_chain_root(stmt.value.func) in stripped_names
        ):
            continue
        # Discarded assignment: _ = database.query(...)
        if (
            isinstance(stmt, ast.Assign)
            and all(isinstance(t, ast.Name) and t.id == "_" for t in stmt.targets)
            and isinstance(stmt.value, ast.Call)
            and _call_chain_root(stmt.value.func) in stripped_names
        ):
            continue
        result.append(stmt)
    return result


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
