"""Tests for dagster AST parsing and generation."""

import ast
from pathlib import Path

from marimo_dagster._dagster_ast import generate_dagster, parse_dagster
from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR, ScriptMetadata

EXAMPLES_DIR = Path(__file__).parent / "examples"


class TestParseDagsterSimple:
    """Tests for parse_dagster with simple_asset.py."""

    def test_single_asset_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 1

    def test_single_asset_name(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].name == "my_data"

    def test_single_asset_no_inputs(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []

    def test_single_asset_outputs(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].outputs == ["my_data"]

    def test_single_asset_return_type(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].return_type_annotation == "dict"

    def test_single_asset_docstring(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "A simple asset that produces static data."

    def test_single_asset_cell_type(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].cell_type == CellType.CODE

    def test_module_docstring(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.module_docstring is not None
        assert "Simple single asset" in ir.module_docstring

    def test_metadata_parsed(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert ir.metadata.requires_python == ">=3.12"
        assert "dagster>=1.9.0" in ir.metadata.dependencies


class TestParseDagsterTwoAssets:
    """Tests for parse_dagster with two_assets_with_dep.py."""

    def test_two_assets_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/two_assets_with_dep.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 2

    def test_upstream_asset(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/two_assets_with_dep.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].name == "raw_data"
        assert ir.cells[0].inputs == []
        assert ir.cells[0].outputs == ["raw_data"]

    def test_downstream_asset_depends_on_upstream(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/two_assets_with_dep.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[1].name == "processed_data"
        assert ir.cells[1].inputs == ["raw_data"]
        assert ir.cells[1].outputs == ["processed_data"]


class TestParseDagsterThreeAssets:
    """Tests for parse_dagster with three_asset_chain.py."""

    def test_three_assets_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/three_asset_chain.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 3

    def test_chain_dependencies(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/three_asset_chain.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []
        assert ir.cells[1].inputs == ["source_data"]
        assert ir.cells[2].inputs == ["filtered_data"]

    def test_chain_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/three_asset_chain.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["source_data", "filtered_data", "final_report"]


class TestParseDagsterImports:
    """Tests for import extraction from dagster modules."""

    def test_non_framework_imports_collected(self) -> None:
        source = (
            'import dagster as dg\n'
            'import polars as pl\n'
            'from pathlib import Path\n'
            '\n'
            '@dg.asset\n'
            'def my_asset() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        modules = [imp.module for imp in ir.imports]
        assert "polars" in modules
        assert "pathlib" in modules

    def test_dagster_import_excluded(self) -> None:
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_asset() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        modules = [imp.module for imp in ir.imports]
        assert "dagster" not in modules


class TestParseDagsterBodyTransform:
    """Tests for dagster function body transformation into IR."""

    def test_body_has_statements(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells[0].body_stmts) > 0

    def test_return_removed_from_body(self) -> None:
        """The return statement should be stripped; IR uses outputs instead."""
        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        for stmt in ir.cells[0].body_stmts:
            assert not isinstance(stmt, ast.Return)


class TestGenerateDagsterStructure:
    """Tests for basic dagster module generation."""

    def test_output_is_valid_python(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="my_data",
                    body_stmts=ast.parse('my_data = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_data"],
                )
            ],
        )
        result = generate_dagster(ir)
        ast.parse(result)

    def test_has_dagster_import(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "import dagster as dg" in result

    def test_has_asset_decorator(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "@dg.asset" in result

    def test_asset_function_name(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="my_data",
                    body_stmts=ast.parse('my_data = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_data"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "def my_data()" in result

    def test_asset_dependencies_as_params(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="raw",
                    body_stmts=ast.parse('raw = {"x": 1}').body,
                    inputs=[],
                    outputs=["raw"],
                ),
                CellNode(
                    name="processed",
                    body_stmts=ast.parse("processed = raw['x'] + 1").body,
                    inputs=["raw"],
                    outputs=["processed"],
                ),
            ],
        )
        result = generate_dagster(ir)
        assert "def processed(raw):" in result

    def test_asset_has_return(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="my_data",
                    body_stmts=ast.parse('my_data = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_data"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "return my_data" in result

    def test_return_type_annotation(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="my_data",
                    body_stmts=ast.parse('my_data = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_data"],
                    return_type_annotation="dict",
                )
            ],
        )
        result = generate_dagster(ir)
        assert "-> dict:" in result

    def test_docstring_preserved(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="my_data",
                    body_stmts=ast.parse('my_data = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_data"],
                    docstring="Produces some data.",
                )
            ],
        )
        result = generate_dagster(ir)
        assert "Produces some data." in result

    def test_non_code_cells_skipped(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="md",
                    body_stmts=[],
                    inputs=[],
                    outputs=[],
                    cell_type=CellType.MARKDOWN,
                ),
                CellNode(
                    name="my_data",
                    body_stmts=ast.parse('my_data = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_data"],
                ),
            ],
        )
        result = generate_dagster(ir)
        # Only one @dg.asset (the CODE cell)
        assert result.count("@dg.asset") == 1


class TestGenerateDagsterImports:
    """Tests for import generation in dagster output."""

    def test_non_framework_imports_included(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="polars", alias="pl")],
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "import polars as pl" in result

    def test_from_imports_included(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="pathlib", names=[("Path", None)])],
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "from pathlib import Path" in result


class TestGenerateDagsterPep723:
    """Tests for PEP 723 metadata in dagster output."""

    def test_metadata_included(self) -> None:
        ir = NotebookIR(
            metadata=ScriptMetadata(
                requires_python=">=3.12",
                dependencies=["dagster>=1.9.0"],
            ),
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "# /// script" in result

    def test_module_docstring_included(self) -> None:
        ir = NotebookIR(
            module_docstring="My module.",
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "My module." in result


class TestGenerateDagsterFromMarimoExamples:
    """Integration tests: parse marimo -> generate dagster."""

    def test_querying_dataframes_produces_valid_dagster(self) -> None:
        from marimo_dagster._marimo_ast import parse_marimo

        source = EXAMPLES_DIR.joinpath("marimo/tier1/querying_dataframes.py").read_text()
        ir = parse_marimo(source)
        result = generate_dagster(ir)
        ast.parse(result)
        assert "import dagster as dg" in result
        assert "@dg.asset" in result

    def test_read_csv_produces_valid_dagster(self) -> None:
        from marimo_dagster._marimo_ast import parse_marimo

        source = EXAMPLES_DIR.joinpath("marimo/tier1/read_csv.py").read_text()
        ir = parse_marimo(source)
        result = generate_dagster(ir)
        ast.parse(result)


class TestParseDagsterEdgeCases:
    """Tests for edge cases in dagster parsing."""

    def test_from_dagster_import_excluded(self) -> None:
        """from dagster import ... should be excluded."""
        source = (
            'from dagster import asset\n'
            '\n'
            '@asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        modules = [imp.module for imp in ir.imports]
        assert "dagster" not in modules

    def test_dagster_asset_call_form(self) -> None:
        """@dg.asset(...) with arguments should be recognized."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset(group_name="test")\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_context_param_filtered(self) -> None:
        """context: dg.AssetExecutionContext should be filtered from inputs."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data(context: dg.AssetExecutionContext) -> dict:\n'
            '    context.log.info("hello")\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []

    def test_empty_body_after_docstring(self) -> None:
        """Asset with only a docstring and no body besides return."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    """My doc."""\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "My doc."
        assert len(ir.cells[0].body_stmts) > 0

    def test_no_return_statement(self) -> None:
        """Asset with no return should still parse."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data():\n'
            '    print("side effect")\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1

    def test_non_asset_function_ignored(self) -> None:
        """Regular functions should be ignored."""
        source = (
            'import dagster as dg\n'
            '\n'
            'def helper():\n'
            '    return 42\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": helper()}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_docstring_only_body(self) -> None:
        """Asset with only a docstring (body is empty after strip)."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data():\n'
            '    """Just a docstring."""\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "Just a docstring."
        assert ir.cells[0].body_stmts == []

    def test_unrecognized_attribute_decorator_ignored(self) -> None:
        """Function with non-dagster attribute decorator should be ignored."""
        source = (
            'import dagster as dg\n'
            'import other\n'
            '\n'
            '@other.asset\n'
            'def not_an_asset():\n'
            '    return 1\n'
            '\n'
            '@dg.asset\n'
            'def real_asset() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "real_asset"

    def test_call_decorator_not_asset(self) -> None:
        """@dg.not_asset(...) should not match."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.op()\n'
            'def my_op():\n'
            '    return 1\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1

    def test_bare_asset_decorator(self) -> None:
        """@asset from `from dagster import asset` should be recognized."""
        source = (
            'from dagster import asset\n'
            '\n'
            '@asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_bare_asset_call_decorator(self) -> None:
        """@asset(...) from `from dagster import asset` should be recognized."""
        source = (
            'from dagster import asset\n'
            '\n'
            '@asset(group_name="test")\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_bare_aliased_asset_decorator(self) -> None:
        """@my_asset from `from dagster import asset as my_asset` should be recognized."""
        source = (
            'from dagster import asset as my_asset\n'
            '\n'
            '@my_asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_bare_non_asset_name_not_matched(self) -> None:
        """@op from `from dagster import op` should NOT be matched as asset."""
        source = (
            'from dagster import asset, op\n'
            '\n'
            '@op\n'
            'def my_op():\n'
            '    return 1\n'
            '\n'
            '@asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_bare_call_non_asset_not_matched(self) -> None:
        """@op(...) call form from dagster should NOT be matched as asset."""
        source = (
            'from dagster import asset, op\n'
            '\n'
            '@op()\n'
            'def my_op():\n'
            '    return 1\n'
            '\n'
            '@asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "my_data"

    def test_relative_import_without_module_ignored(self) -> None:
        """from . import x (no module name) should not crash."""
        source = (
            'import dagster as dg\n'
            'from . import helpers\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        # Relative import with no module should be silently skipped
        assert all(imp.module != "" for imp in ir.imports)

    def test_call_decorator_wrong_module(self) -> None:
        """@other.asset(...) call form should not match."""
        source = (
            'import dagster as dg\n'
            'import other\n'
            '\n'
            '@other.asset()\n'
            'def not_an_asset():\n'
            '    return 1\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1


class TestGenerateDagsterEdgeCases:
    """Tests for edge cases in dagster generation."""

    def test_from_import_with_alias(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="pathlib", names=[("Path", "P")])],
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "from pathlib import Path as P" in result

    def test_plain_import_no_alias(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="os")],
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "import os" in result

    def test_cell_without_outputs(self) -> None:
        """A cell with no outputs should not have a return statement."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="side_effect",
                    body_stmts=ast.parse("print('hello')").body,
                    inputs=[],
                    outputs=[],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "return" not in result.split("def side_effect")[1]
