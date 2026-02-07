"""Tests for dagster AST parsing and generation."""

import ast

from conftest import EXAMPLES_DIR
from marimo_dagster._dagster_ast import generate_dagster, parse_dagster
from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR, ScriptMetadata


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
        # Cell returns (df, data) so it becomes a multi_asset
        assert "@dg.multi_asset" in result

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


class TestParseDagsterTier2Definitions:
    """Tests for parse_dagster with assets_with_definitions.py."""

    def test_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_definitions.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 3

    def test_asset_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_definitions.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["users", "products", "user_product_summary"]

    def test_fan_in_dependencies(self) -> None:
        """user_product_summary depends on both users and products."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_definitions.py").read_text()
        ir = parse_dagster(source)
        summary = ir.cells[2]
        assert set(summary.inputs) == {"users", "products"}

    def test_root_assets_have_no_inputs(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_definitions.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []
        assert ir.cells[1].inputs == []

    def test_definitions_block_ignored(self) -> None:
        """The dg.Definitions(...) call should not produce cells or imports."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_definitions.py").read_text()
        ir = parse_dagster(source)
        # Only asset functions become cells
        assert len(ir.cells) == 3
        assert all(c.cell_type == CellType.CODE for c in ir.cells)


class TestParseDagsterTier2Resources:
    """Tests for parse_dagster with assets_with_resources.py."""

    def test_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 2

    def test_asset_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["raw_events", "event_counts"]

    def test_resource_class_ignored(self) -> None:
        """ConfigurableResource class definition should not produce a cell."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        cell_names = [c.name for c in ir.cells]
        assert "DatabaseResource" not in cell_names

    def test_resource_param_filtered(self) -> None:
        """Resource params should be filtered from inputs.

        The `database: DatabaseResource` parameter is a dagster resource,
        not a data dependency. It should not appear in inputs.
        """
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        assert "database" not in ir.cells[0].inputs
        assert "database" not in ir.cells[1].inputs

    def test_resource_body_calls_stripped(self) -> None:
        """Bare calls and discarded assignments to resource vars are stripped."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        # raw_events: `_ = database.query(...)` should be stripped
        body0 = "\n".join(ast.unparse(s) for s in ir.cells[0].body_stmts)
        assert "database.query" not in body0
        # event_counts: `database.execute(...)` should be stripped
        body1 = "\n".join(ast.unparse(s) for s in ir.cells[1].body_stmts)
        assert "database.execute" not in body1

    def test_data_dependency_preserved(self) -> None:
        """event_counts depends on raw_events (a real data dependency)."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        assert "raw_events" in ir.cells[1].inputs

    def test_from_dagster_import_excluded(self) -> None:
        """The `from dagster import ConfigurableResource` should be excluded."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_resources.py").read_text()
        ir = parse_dagster(source)
        modules = [imp.module for imp in ir.imports]
        assert "dagster" not in modules


class TestParseDagsterTier2JobsSchedules:
    """Tests for parse_dagster with assets_with_jobs_schedules.py."""

    def test_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_jobs_schedules.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 3

    def test_asset_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_jobs_schedules.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["daily_sales", "daily_inventory", "weekly_report"]

    def test_fan_in_dependency(self) -> None:
        """weekly_report depends on both daily_sales and daily_inventory."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_jobs_schedules.py").read_text()
        ir = parse_dagster(source)
        assert set(ir.cells[2].inputs) == {"daily_sales", "daily_inventory"}

    def test_jobs_schedules_ignored(self) -> None:
        """Jobs, schedules, and AssetSelection should not produce cells."""
        source = EXAMPLES_DIR.joinpath("dagster/tier2/assets_with_jobs_schedules.py").read_text()
        ir = parse_dagster(source)
        cell_names = [c.name for c in ir.cells]
        assert "daily_update_job" not in cell_names
        assert "weekly_update_job" not in cell_names
        assert "daily_schedule" not in cell_names
        assert "weekly_schedule" not in cell_names


class TestParseDagsterTier3Diamond:
    """Tests for parse_dagster with diamond_dependency.py."""

    def test_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/diamond_dependency.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 4

    def test_asset_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/diamond_dependency.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["source", "left_branch", "right_branch", "merged"]

    def test_diamond_fan_out(self) -> None:
        """Both left_branch and right_branch depend on source."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/diamond_dependency.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[1].inputs == ["source"]
        assert ir.cells[2].inputs == ["source"]

    def test_diamond_fan_in(self) -> None:
        """merged depends on both left_branch and right_branch."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/diamond_dependency.py").read_text()
        ir = parse_dagster(source)
        assert set(ir.cells[3].inputs) == {"left_branch", "right_branch"}

    def test_root_has_no_inputs(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/diamond_dependency.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []

    def test_definitions_block_ignored(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/diamond_dependency.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 4


class TestParseDagsterTier3GroupsMetadata:
    """Tests for parse_dagster with groups_and_metadata.py."""

    def test_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 3

    def test_asset_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["api_data", "transformed_data", "summary_stats"]

    def test_context_param_filtered(self) -> None:
        """context: dg.AssetExecutionContext should be filtered from all assets."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        for cell in ir.cells:
            assert "context" not in cell.inputs

    def test_data_dependency_chain(self) -> None:
        """api_data -> transformed_data -> summary_stats."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []
        assert ir.cells[1].inputs == ["api_data"]
        assert ir.cells[2].inputs == ["transformed_data"]

    def test_non_dagster_import_preserved(self) -> None:
        """pandas import should be preserved."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        modules = [imp.module for imp in ir.imports]
        assert "pandas" in modules

    def test_pandas_alias_preserved(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        pd_import = next(i for i in ir.imports if i.module == "pandas")
        assert pd_import.alias == "pd"

    def test_decorator_kwargs_preserved(self) -> None:
        """group_name, compute_kind, description should be in decorator_kwargs."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        # api_data: group_name="ingestion", compute_kind="API"
        assert ir.cells[0].decorator_kwargs["group_name"] == "ingestion"
        assert ir.cells[0].decorator_kwargs["compute_kind"] == "API"
        # summary_stats: group_name="reporting", compute_kind="Analysis", description=...
        assert ir.cells[2].decorator_kwargs["group_name"] == "reporting"
        assert ir.cells[2].decorator_kwargs["compute_kind"] == "Analysis"
        assert "summary statistics" in ir.cells[2].decorator_kwargs["description"]

    def test_context_body_calls_stripped(self) -> None:
        """context.add_output_metadata() and context.log.info() should be stripped."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        for cell in ir.cells:
            body = "\n".join(ast.unparse(s) for s in cell.body_stmts)
            assert "context.add_output_metadata" not in body
            assert "context.log.info" not in body

    def test_docstrings_preserved(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/groups_and_metadata.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].docstring is not None
        assert "API" in ir.cells[0].docstring


class TestParseDagsterTier3Partitions:
    """Tests for parse_dagster with partitioned_assets.py."""

    def test_cell_count(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 3

    def test_asset_names(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        names = [c.name for c in ir.cells]
        assert names == ["monthly_sales_data", "weekly_metrics", "monthly_sales_report"]

    def test_context_param_filtered(self) -> None:
        """All three assets take context — it should be filtered."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        for cell in ir.cells:
            assert "context" not in cell.inputs

    def test_data_dependency(self) -> None:
        """monthly_sales_report depends on monthly_sales_data."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[2].inputs == ["monthly_sales_data"]

    def test_independent_partitioned_assets(self) -> None:
        """monthly_sales_data and weekly_metrics are independent roots."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == []
        assert ir.cells[1].inputs == []

    def test_partition_defs_ignored(self) -> None:
        """MonthlyPartitionsDefinition etc. should not produce cells."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        cell_names = [c.name for c in ir.cells]
        assert "monthly_partition" not in cell_names
        assert "weekly_partition" not in cell_names

    def test_context_body_calls_stripped(self) -> None:
        """context.log.info() calls should be stripped from body."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/partitioned_assets.py").read_text()
        ir = parse_dagster(source)
        for cell in ir.cells:
            body = "\n".join(ast.unparse(s) for s in cell.body_stmts)
            assert "context.log.info" not in body


class TestParseDagsterTier3Sensor:
    """Tests for parse_dagster with assets_with_sensor.py."""

    def test_only_asset_extracted(self) -> None:
        """Only adhoc_request is an @dg.asset; the sensor function is not."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/assets_with_sensor.py").read_text()
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].name == "adhoc_request"

    def test_sensor_function_ignored(self) -> None:
        """@dg.sensor decorated function should not be treated as an asset."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/assets_with_sensor.py").read_text()
        ir = parse_dagster(source)
        cell_names = [c.name for c in ir.cells]
        assert "adhoc_request_sensor" not in cell_names

    def test_non_dagster_imports_preserved(self) -> None:
        """json and os imports should be preserved."""
        source = EXAMPLES_DIR.joinpath("dagster/tier3/assets_with_sensor.py").read_text()
        ir = parse_dagster(source)
        modules = [imp.module for imp in ir.imports]
        assert "json" in modules
        assert "os" in modules

    def test_job_definition_ignored(self) -> None:
        source = EXAMPLES_DIR.joinpath("dagster/tier3/assets_with_sensor.py").read_text()
        ir = parse_dagster(source)
        cell_names = [c.name for c in ir.cells]
        assert "adhoc_request_job" not in cell_names


class TestDecoratorDescription:
    """Tests for extracting @dg.asset(description=...) as docstring."""

    def test_description_used_when_no_docstring(self) -> None:
        """description= kwarg should become docstring if function has none."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset(description="Loads raw data from source.")\n'
            'def raw_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "Loads raw data from source."

    def test_existing_docstring_takes_precedence(self) -> None:
        """If function has a docstring, description= should NOT overwrite it."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset(description="Decorator description.")\n'
            'def raw_data() -> dict:\n'
            '    """Function docstring."""\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "Function docstring."
        # description still in decorator_kwargs
        assert ir.cells[0].decorator_kwargs["description"] == "Decorator description."


class TestGenerateDagsterDecoratorKwargs:
    """Tests for generating @dg.asset(...) with keyword arguments."""

    def test_kwargs_emitted_in_decorator(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1").body,
                    inputs=[],
                    outputs=["x"],
                    decorator_kwargs={"group_name": "ingestion", "compute_kind": "API"},
                )
            ],
        )
        result = generate_dagster(ir)
        assert 'group_name="ingestion"' in result
        assert 'compute_kind="API"' in result

    def test_no_kwargs_plain_decorator(self) -> None:
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
        assert "@dg.asset\n" in result


class TestResourceParamAttributeForm:
    """Tests for resource classes using dg.ConfigurableResource attribute form."""

    def test_resource_via_dg_attribute(self) -> None:
        """class Foo(dg.ConfigurableResource) should be detected as resource."""
        source = (
            'import dagster as dg\n'
            '\n'
            'class MyResource(dg.ConfigurableResource):\n'
            '    url: str\n'
            '\n'
            '@dg.asset\n'
            'def my_data(resource: MyResource) -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert "resource" not in ir.cells[0].inputs

    def test_non_dagster_resource_attribute_not_detected(self) -> None:
        """class Foo(other.ConfigurableResource) should NOT be treated as resource."""
        source = (
            'import dagster as dg\n'
            'import other\n'
            '\n'
            'class MyResource(other.ConfigurableResource):\n'
            '    url: str\n'
            '\n'
            '@dg.asset\n'
            'def my_data(resource: MyResource) -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        # MyResource is NOT from dagster, so resource param is kept as input
        assert "resource" in ir.cells[0].inputs

    def test_call_chain_root_non_name(self) -> None:
        """_call_chain_root should return None for non-Name roots."""
        from marimo_dagster._dagster_ast import _call_chain_root

        # func()[0].method() — root is a Call, not a Name
        node = ast.parse("func()[0].method()").body[0].value.func  # type: ignore[union-attr]
        assert _call_chain_root(node) is None


class TestDecoratorKwargsOnlyFromAsset:
    """Tests that _extract_decorator_kwargs only reads from @dg.asset decorators."""

    def test_non_asset_decorator_kwargs_ignored(self) -> None:
        """A non-asset decorator's kwargs should not be extracted."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@my_wrapper(description="not an asset description")\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        # description from @my_wrapper should NOT be treated as asset metadata
        assert ir.cells[0].decorator_kwargs == {}

    def test_asset_decorator_kwargs_still_extracted(self) -> None:
        """@dg.asset(group_name=...) kwargs should still be extracted."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@my_wrapper(description="wrapper desc")\n'
            '@dg.asset(group_name="ingestion")\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].decorator_kwargs == {"group_name": "ingestion"}

    def test_non_asset_description_not_used_as_docstring(self) -> None:
        """description from a non-asset decorator should not become docstring."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@my_wrapper(description="wrapper desc")\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring is None

    def test_attribute_call_non_asset_ignored(self) -> None:
        """@other.decorator(key="val") should not be extracted as asset kwargs."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@other.decorator(key="val")\n'
            '@dg.asset(group_name="data")\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].decorator_kwargs == {"group_name": "data"}

    def test_non_dagster_module_asset_call_ignored(self) -> None:
        """@other.asset(key="val") should not be extracted as asset kwargs."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@other.asset(key="val")\n'
            '@dg.asset(compute_kind="SQL")\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].decorator_kwargs == {"compute_kind": "SQL"}


class TestGenerateMultiAsset:
    """Tests for generating @dg.multi_asset for cells with multiple outputs."""

    def test_multi_output_uses_multi_asset_decorator(self) -> None:
        """Cells with multiple outputs should use @dg.multi_asset."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nfiltered_df = 2").body,
                    inputs=[],
                    outputs=["chart", "filtered_df"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "@dg.multi_asset" in result

    def test_multi_output_has_outs_dict(self) -> None:
        """multi_asset should declare outs for each output."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nfiltered_df = 2").body,
                    inputs=[],
                    outputs=["chart", "filtered_df"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert '"chart": dg.AssetOut()' in result
        assert '"filtered_df": dg.AssetOut()' in result

    def test_multi_output_returns_tuple(self) -> None:
        """multi_asset function should return all outputs as a tuple."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nfiltered_df = 2").body,
                    inputs=[],
                    outputs=["chart", "filtered_df"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "return chart, filtered_df" in result

    def test_multi_output_valid_python(self) -> None:
        """Generated multi_asset code should be valid Python."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nfiltered_df = 2").body,
                    inputs=[],
                    outputs=["chart", "filtered_df"],
                )
            ],
        )
        result = generate_dagster(ir)
        ast.parse(result)

    def test_single_output_still_uses_asset(self) -> None:
        """Cells with a single output should still use @dg.asset."""
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
        assert "@dg.asset\n" in result
        assert "@dg.multi_asset" not in result

    def test_multi_output_with_inputs(self) -> None:
        """multi_asset with inputs should list them as function params."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = raw + 1\nstats = raw + 2").body,
                    inputs=["raw"],
                    outputs=["chart", "stats"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "def chart(raw):" in result

    def test_multi_output_with_decorator_kwargs(self) -> None:
        """multi_asset should include decorator_kwargs alongside outs."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nstats = 2").body,
                    inputs=[],
                    outputs=["chart", "stats"],
                    decorator_kwargs={"group_name": "analytics"},
                )
            ],
        )
        result = generate_dagster(ir)
        assert "@dg.multi_asset" in result
        assert 'group_name="analytics"' in result
        assert '"chart": dg.AssetOut()' in result

    def test_multi_output_three_outputs(self) -> None:
        """multi_asset should handle three or more outputs."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="a",
                    body_stmts=ast.parse("a = 1\nb = 2\nc = 3").body,
                    inputs=[],
                    outputs=["a", "b", "c"],
                )
            ],
        )
        result = generate_dagster(ir)
        assert "@dg.multi_asset" in result
        assert '"a": dg.AssetOut()' in result
        assert '"b": dg.AssetOut()' in result
        assert '"c": dg.AssetOut()' in result
        assert "return a, b, c" in result

    def test_multi_output_with_docstring(self) -> None:
        """multi_asset should include docstring."""
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nstats = 2").body,
                    inputs=[],
                    outputs=["chart", "stats"],
                    docstring="Produce chart and stats.",
                )
            ],
        )
        result = generate_dagster(ir)
        assert "Produce chart and stats." in result


class TestParseMultiAsset:
    """Tests for parsing @dg.multi_asset decorated functions."""

    def test_parse_multi_asset_cell_count(self) -> None:
        """A @dg.multi_asset function should produce one cell."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "chart": dg.AssetOut(),\n'
            '        "filtered_df": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def chart(raw):\n'
            '    chart = raw + 1\n'
            '    filtered_df = raw + 2\n'
            '    return chart, filtered_df\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1

    def test_parse_multi_asset_outputs(self) -> None:
        """Outputs should be extracted from the outs dict keys."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "chart": dg.AssetOut(),\n'
            '        "filtered_df": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def chart(raw):\n'
            '    chart = raw + 1\n'
            '    filtered_df = raw + 2\n'
            '    return chart, filtered_df\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].outputs == ["chart", "filtered_df"]

    def test_parse_multi_asset_inputs(self) -> None:
        """Inputs should be extracted from function params."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "chart": dg.AssetOut(),\n'
            '        "filtered_df": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def chart(raw):\n'
            '    chart = raw + 1\n'
            '    filtered_df = raw + 2\n'
            '    return chart, filtered_df\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == ["raw"]

    def test_parse_multi_asset_name(self) -> None:
        """Cell name should be the function name."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "chart": dg.AssetOut(),\n'
            '        "filtered_df": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def chart(raw):\n'
            '    chart = raw + 1\n'
            '    filtered_df = raw + 2\n'
            '    return chart, filtered_df\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].name == "chart"

    def test_parse_multi_asset_body_no_return(self) -> None:
        """Return statement should be stripped from body."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    a = 1\n'
            '    b = 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        for stmt in ir.cells[0].body_stmts:
            assert not isinstance(stmt, ast.Return)

    def test_parse_multi_asset_with_context(self) -> None:
        """context param should be filtered from multi_asset inputs."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a(context: dg.AssetExecutionContext, raw):\n'
            '    a = raw + 1\n'
            '    b = raw + 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].inputs == ["raw"]

    def test_parse_multi_asset_roundtrip(self) -> None:
        """Generating then parsing a multi_asset should preserve structure."""
        original_ir = NotebookIR(
            cells=[
                CellNode(
                    name="chart",
                    body_stmts=ast.parse("chart = 1\nfiltered_df = 2").body,
                    inputs=["raw"],
                    outputs=["chart", "filtered_df"],
                )
            ],
        )
        dagster_source = generate_dagster(original_ir)
        parsed_ir = parse_dagster(dagster_source)
        assert len(parsed_ir.cells) == 1
        assert parsed_ir.cells[0].name == "chart"
        assert parsed_ir.cells[0].outputs == ["chart", "filtered_df"]
        assert parsed_ir.cells[0].inputs == ["raw"]

    def test_parse_multi_asset_with_docstring(self) -> None:
        """Docstring should be extracted from multi_asset function."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    """Produce a and b."""\n'
            '    a = 1\n'
            '    b = 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "Produce a and b."

    def test_mixed_asset_and_multi_asset(self) -> None:
        """Both @dg.asset and @dg.multi_asset should be parsed."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def raw() -> dict:\n'
            '    return {"x": 1}\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "chart": dg.AssetOut(),\n'
            '        "stats": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def chart(raw):\n'
            '    chart = raw["x"] + 1\n'
            '    stats = raw["x"] + 2\n'
            '    return chart, stats\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 2
        assert ir.cells[0].name == "raw"
        assert ir.cells[0].outputs == ["raw"]
        assert ir.cells[1].name == "chart"
        assert ir.cells[1].outputs == ["chart", "stats"]

    def test_parse_multi_asset_with_extra_decorators(self) -> None:
        """Non-call decorators on multi_asset should be ignored gracefully."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@some_wrapper\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    a = 1\n'
            '    b = 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert ir.cells[0].outputs == ["a", "b"]

    def test_parse_multi_asset_with_non_multi_asset_call_decorator(self) -> None:
        """@other.decorator() alongside @dg.multi_asset should be handled."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@other.decorator(key="val")\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    a = 1\n'
            '    b = 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].outputs == ["a", "b"]

    def test_parse_multi_asset_docstring_only_body(self) -> None:
        """multi_asset with only docstring and return should have empty body."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    """Just docs."""\n'
            '    return 1, 2\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].docstring == "Just docs."
        assert ir.cells[0].body_stmts == []

    def test_parse_multi_asset_no_return(self) -> None:
        """multi_asset body with no return statement should still parse."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    print("side effect")\n'
        )
        ir = parse_dagster(source)
        assert len(ir.cells) == 1
        assert len(ir.cells[0].body_stmts) == 1

    def test_parse_multi_asset_docstring_only_no_return(self) -> None:
        """multi_asset with only docstring (no return) should have empty body."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    """Just docs."""\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].body_stmts == []

    def test_parse_multi_asset_without_outs_kwarg(self) -> None:
        """multi_asset without outs kwarg should produce empty outputs."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset()\n'
            'def a():\n'
            '    a = 1\n'
            '    return a\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].outputs == []

    def test_parse_multi_asset_with_extra_kwargs(self) -> None:
        """multi_asset with group_name alongside outs should parse outs."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.multi_asset(\n'
            '    group_name="analytics",\n'
            '    outs={\n'
            '        "a": dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    },\n'
            ')\n'
            'def a():\n'
            '    a = 1\n'
            '    b = 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        assert ir.cells[0].outputs == ["a", "b"]

    def test_parse_multi_asset_non_string_key_in_outs(self) -> None:
        """Non-string keys in outs dict should be skipped."""
        source = (
            'import dagster as dg\n'
            '\n'
            'NAME = "dynamic"\n'
            '@dg.multi_asset(\n'
            '    outs={\n'
            '        NAME: dg.AssetOut(),\n'
            '        "b": dg.AssetOut(),\n'
            '    }\n'
            ')\n'
            'def a():\n'
            '    a = 1\n'
            '    b = 2\n'
            '    return a, b\n'
        )
        ir = parse_dagster(source)
        # Only "b" is extractable; NAME is a variable reference, not a string literal
        assert ir.cells[0].outputs == ["b"]
