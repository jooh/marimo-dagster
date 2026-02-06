"""Tests for marimo AST parsing and generation."""

import ast
from pathlib import Path
from unittest.mock import patch

from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR, ScriptMetadata
from marimo_dagster._marimo_ast import _marimo_version, generate_marimo, parse_marimo

EXAMPLES_DIR = Path(__file__).parent / "examples"


class TestGenerateMarimoStructure:
    """Tests for basic marimo structure generation."""

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
        result = generate_marimo(ir)
        ast.parse(result)  # Should not raise

    def test_has_marimo_import(self) -> None:
        ir = NotebookIR(cells=[])
        result = generate_marimo(ir)
        assert "import marimo" in result

    def test_has_generated_with(self) -> None:
        ir = NotebookIR(cells=[])
        result = generate_marimo(ir)
        assert "__generated_with" in result

    def test_has_app_creation(self) -> None:
        ir = NotebookIR(cells=[])
        result = generate_marimo(ir)
        assert "app = marimo.App(" in result

    def test_has_main_guard(self) -> None:
        ir = NotebookIR(cells=[])
        result = generate_marimo(ir)
        assert 'if __name__ == "__main__"' in result
        assert "app.run()" in result

    def test_has_app_cell_decorator(self) -> None:
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
        result = generate_marimo(ir)
        assert "@app.cell" in result


class TestGenerateMarimoImportCell:
    """Tests for the generated import cell."""

    def test_import_cell_with_plain_import(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="polars", alias="pl")],
            cells=[],
        )
        result = generate_marimo(ir)
        assert "import polars as pl" in result

    def test_import_cell_with_from_import(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="pathlib", names=[("Path", None)])],
            cells=[],
        )
        result = generate_marimo(ir)
        assert "from pathlib import Path" in result

    def test_import_cell_includes_marimo_as_mo(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="polars", alias="pl")],
            cells=[],
        )
        result = generate_marimo(ir)
        assert "import marimo as mo" in result

    def test_import_cell_returns_names(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="polars", alias="pl")],
            cells=[],
        )
        result = generate_marimo(ir)
        assert "return (mo, pl)" in result or "return (pl, mo)" in result


class TestGenerateMarimoCells:
    """Tests for code cell generation."""

    def test_cell_dependencies_as_params(self) -> None:
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
        result = generate_marimo(ir)
        assert "def _(raw):" in result

    def test_cell_returns_outputs(self) -> None:
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
        result = generate_marimo(ir)
        assert "return (x,)" in result

    def test_cell_no_outputs_bare_return(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="display",
                    body_stmts=ast.parse("print('hello')").body,
                    inputs=[],
                    outputs=[],
                )
            ],
        )
        result = generate_marimo(ir)
        assert "return\n" in result or "return\n" in result

    def test_docstring_becomes_markdown_cell(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="my_asset",
                    body_stmts=ast.parse('my_asset = {"x": 1}').body,
                    inputs=[],
                    outputs=["my_asset"],
                    docstring="This produces data.",
                )
            ],
        )
        result = generate_marimo(ir)
        assert "mo.md" in result
        assert "This produces data." in result


class TestGenerateMarimoPep723:
    """Tests for PEP 723 metadata in generated output."""

    def test_metadata_included(self) -> None:
        ir = NotebookIR(
            metadata=ScriptMetadata(
                requires_python=">=3.12",
                dependencies=["dagster>=1.9.0"],
            ),
            cells=[],
        )
        result = generate_marimo(ir)
        assert "# /// script" in result

    def test_no_metadata_when_empty(self) -> None:
        ir = NotebookIR(cells=[])
        result = generate_marimo(ir)
        assert "# /// script" not in result

    def test_module_docstring_included(self) -> None:
        ir = NotebookIR(
            module_docstring="My module documentation.",
            cells=[],
        )
        result = generate_marimo(ir)
        assert "My module documentation." in result


class TestGenerateMarimoFromDagsterExamples:
    """Integration tests: parse dagster -> generate marimo."""

    def test_simple_asset_produces_valid_marimo(self) -> None:
        from marimo_dagster._dagster_ast import parse_dagster

        source = EXAMPLES_DIR.joinpath("dagster/tier1/simple_asset.py").read_text()
        ir = parse_dagster(source)
        result = generate_marimo(ir)
        ast.parse(result)  # valid Python
        assert "import marimo" in result
        assert "@app.cell" in result
        assert "my_data" in result

    def test_two_assets_produces_valid_marimo(self) -> None:
        from marimo_dagster._dagster_ast import parse_dagster

        source = EXAMPLES_DIR.joinpath("dagster/tier1/two_assets_with_dep.py").read_text()
        ir = parse_dagster(source)
        result = generate_marimo(ir)
        ast.parse(result)
        assert "raw_data" in result
        assert "processed_data" in result

    def test_three_asset_chain_produces_valid_marimo(self) -> None:
        from marimo_dagster._dagster_ast import parse_dagster

        source = EXAMPLES_DIR.joinpath("dagster/tier1/three_asset_chain.py").read_text()
        ir = parse_dagster(source)
        result = generate_marimo(ir)
        ast.parse(result)
        assert "source_data" in result
        assert "filtered_data" in result
        assert "final_report" in result

    def test_dependencies_preserved_as_cell_params(self) -> None:
        from marimo_dagster._dagster_ast import parse_dagster

        source = EXAMPLES_DIR.joinpath("dagster/tier1/three_asset_chain.py").read_text()
        ir = parse_dagster(source)
        result = generate_marimo(ir)
        assert "def _(source_data):" in result
        assert "def _(filtered_data):" in result


class TestParseMarimoStructure:
    """Tests for parse_marimo basic structure extraction."""

    def test_module_docstring(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/querying_dataframes.py").read_text()
        ir = parse_marimo(source)
        assert ir.module_docstring is not None
        assert "Query DataFrames" in ir.module_docstring

    def test_metadata_parsed(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/querying_dataframes.py").read_text()
        ir = parse_marimo(source)
        assert ir.metadata.requires_python == ">=3.10"
        assert "marimo" in ir.metadata.dependencies


class TestParseMarimoCellClassification:
    """Tests for cell type classification."""

    def test_markdown_cell_detected(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    mo.md("# Hello")\n'
            '    return\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        md_cells = [c for c in ir.cells if c.cell_type == CellType.MARKDOWN]
        assert len(md_cells) == 1

    def test_import_cell_detected(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    import marimo as mo\n'
            '    import polars as pl\n'
            '    return (mo, pl)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        # Import cell should contribute to ir.imports, not ir.cells
        assert any(imp.module == "polars" for imp in ir.imports)
        # marimo is filtered as framework
        assert not any(imp.module == "marimo" for imp in ir.imports)
        # No code cells created
        assert len(ir.cells) == 0

    def test_sql_cell_detected(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    result = mo.sql(f"SELECT 1")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        sql_cells = [c for c in ir.cells if c.cell_type == CellType.SQL]
        assert len(sql_cells) == 1

    def test_display_only_cell_detected(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(result):\n'
            '    result\n'
            '    return\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        display_cells = [c for c in ir.cells if c.cell_type == CellType.DISPLAY_ONLY]
        assert len(display_cells) == 1

    def test_code_cell_detected(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    from vega_datasets import data\n'
            '    df = data.iris()\n'
            '    return (df, data)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == 1

    def test_ui_cell_detected(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    mo.accordion({"tip": mo.md("text")})\n'
            '    return\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        ui_cells = [c for c in ir.cells if c.cell_type == CellType.UI]
        assert len(ui_cells) == 1


class TestParseMarimoCodeCells:
    """Tests for code cell extraction details."""

    def test_code_cell_name_from_output(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 42\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert code_cells[0].name == "x"
        assert code_cells[0].outputs == ["x"]

    def test_code_cell_inputs(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(x):\n'
            '    y = x + 1\n'
            '    return (y,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert code_cells[0].inputs == ["x"]

    def test_mo_param_filtered_from_inputs(self) -> None:
        """The 'mo' parameter is framework, not a data dependency."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo, df):\n'
            '    result = mo.sql(f"SELECT * FROM df")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        sql_cells = [c for c in ir.cells if c.cell_type == CellType.SQL]
        assert "mo" not in sql_cells[0].inputs
        assert "df" in sql_cells[0].inputs

    def test_code_cell_body_no_return(self) -> None:
        """Body should not contain the return statement."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 42\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        for stmt in code_cells[0].body_stmts:
            assert not isinstance(stmt, ast.Return)


class TestParseMarimoImports:
    """Tests for import extraction from marimo notebooks."""

    def test_pure_import_cell_extracted(self) -> None:
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    import marimo as mo\n'
            '    import polars as pl\n'
            '    return (mo, pl)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        modules = [imp.module for imp in ir.imports]
        assert "polars" in modules

    def test_marimo_import_filtered(self) -> None:
        """'import marimo as mo' should not appear in ir.imports."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    import marimo as mo\n'
            '    return (mo,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        modules = [imp.module for imp in ir.imports]
        assert "marimo" not in modules


class TestParseMarimoWithExamples:
    """Integration tests with tier 1 marimo examples."""

    def test_querying_dataframes_has_code_cell(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/querying_dataframes.py").read_text()
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) >= 1
        # The cell that creates df from data.iris()
        names = [c.name for c in code_cells]
        assert "df" in names

    def test_querying_dataframes_has_sql_cells(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/querying_dataframes.py").read_text()
        ir = parse_marimo(source)
        sql_cells = [c for c in ir.cells if c.cell_type == CellType.SQL]
        assert len(sql_cells) >= 1

    def test_querying_dataframes_has_markdown_cells(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/querying_dataframes.py").read_text()
        ir = parse_marimo(source)
        md_cells = [c for c in ir.cells if c.cell_type == CellType.MARKDOWN]
        assert len(md_cells) >= 1

    def test_read_csv_parsed(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/read_csv.py").read_text()
        ir = parse_marimo(source)
        assert ir.module_docstring is not None
        assert len(ir.cells) > 0

    def test_read_parquet_parsed(self) -> None:
        source = EXAMPLES_DIR.joinpath("marimo/tier1/read_parquet.py").read_text()
        ir = parse_marimo(source)
        assert ir.module_docstring is not None
        assert len(ir.cells) > 0


class TestParseMarimoEdgeCases:
    """Tests for edge cases in marimo parsing."""

    def test_non_cell_function_ignored(self) -> None:
        """Regular functions (not @app.cell) should be ignored."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            'def helper():\n'
            '    return 42\n'
            '@app.cell\n'
            'def _():\n'
            '    x = helper()\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == 1

    def test_cell_without_return(self) -> None:
        """Cell body that doesn't end with return."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    print("hello")\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        assert len(ir.cells) == 1

    def test_empty_cell(self) -> None:
        """Empty cell body."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    pass\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        assert len(ir.cells) == 1

    def test_return_non_name_non_tuple(self) -> None:
        """Return a literal expression (not a name or tuple)."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    return 42\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        # Outputs should be empty since 42 is not a Name
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert code_cells[0].outputs == []

    def test_tuple_with_non_name_element(self) -> None:
        """Return tuple where an element is not a Name."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x, 42)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        # Only x is a Name, 42 is not
        assert "x" in code_cells[0].outputs

    def test_infer_name_underscore_prefix_fallback(self) -> None:
        """Outputs with underscore prefix should fall back to cell_N."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    _private = 1\n'
            '    return (_private,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert code_cells[0].name == "cell_0"

    def test_from_import_in_import_cell(self) -> None:
        """from-import statements in import cells should be extracted."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    import marimo as mo\n'
            '    from pathlib import Path\n'
            '    return (mo, Path)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        modules = [imp.module for imp in ir.imports]
        assert "pathlib" in modules

    def test_mo_ui_widget_detected(self) -> None:
        """mo.ui.* should be detected as UI."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    slider = mo.ui.slider(0, 10)\n'
            '    return\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        ui_cells = [c for c in ir.cells if c.cell_type == CellType.UI]
        assert len(ui_cells) == 1

    def test_is_top_level_mo_call_not_expr(self) -> None:
        """Non-Expr statement should not match mo.md pattern."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    x = mo.md("text")\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        # Should be CODE (assignment), not MARKDOWN
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == 1

    def test_is_top_level_not_call(self) -> None:
        """Bare expression that is not a call should not match mo.md."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    mo\n'
            '    return\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        # Single bare name expression with no outputs — display_only
        display_cells = [c for c in ir.cells if c.cell_type == CellType.DISPLAY_ONLY]
        assert len(display_cells) == 1

    def test_from_marimo_import_filtered_in_import_cell(self) -> None:
        """from marimo import ... should be filtered in import cells."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    from marimo import App\n'
            '    import polars as pl\n'
            '    return (pl,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        modules = [imp.module for imp in ir.imports]
        assert "marimo" not in modules
        assert "polars" in modules

    def test_empty_body_extract_outputs(self) -> None:
        """_extract_outputs with truly empty body returns []."""
        from marimo_dagster._marimo_ast import _extract_outputs

        assert _extract_outputs([]) == []

    def test_non_matching_attribute_decorator(self) -> None:
        """@other.cell should not match as app.cell."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@other.cell\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            '@app.cell\n'
            'def _():\n'
            '    y = 2\n'
            '    return (y,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        # Only the @app.cell function should be detected
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == 1
        assert code_cells[0].name == "y"

    def test_call_decorator_not_cell(self) -> None:
        """@app.function() call form should not match."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.function()\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            '@app.cell\n'
            'def _():\n'
            '    y = 2\n'
            '    return (y,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == 1

    def test_call_decorator_wrong_object(self) -> None:
        """@other.cell() should not match."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@other.cell()\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            '@app.cell\n'
            'def _():\n'
            '    y = 2\n'
            '    return (y,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        ir = parse_marimo(source)
        code_cells = [c for c in ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == 1

    def test_is_top_level_expr_not_call(self) -> None:
        """Expr wrapping a non-Call (like a Name) should not match mo.md."""
        from marimo_dagster._marimo_ast import _is_top_level_mo_call

        # Create an Expr with a Name (not a Call)
        stmt = ast.parse("x").body[0]  # Expr(value=Name(id='x'))
        assert not _is_top_level_mo_call(stmt, "md")


class TestGenerateMarimoEdgeCases:
    """Tests for edge cases in marimo generation."""

    def test_from_import_with_alias(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="pathlib", names=[("Path", "P")])],
            cells=[],
        )
        result = generate_marimo(ir)
        assert "from pathlib import Path as P" in result

    def test_plain_import_no_alias(self) -> None:
        ir = NotebookIR(
            imports=[ImportItem(module="os")],
            cells=[],
        )
        result = generate_marimo(ir)
        assert "import os" in result
        assert "return (mo, os)" in result

    def test_multi_output_cell(self) -> None:
        ir = NotebookIR(
            cells=[
                CellNode(
                    name="x",
                    body_stmts=ast.parse("x = 1\ny = 2").body,
                    inputs=[],
                    outputs=["x", "y"],
                )
            ],
        )
        result = generate_marimo(ir)
        assert "return (x, y)" in result


class TestMarimoVersion:
    """Tests for the _marimo_version helper."""

    def test_returns_installed_version(self) -> None:
        result = _marimo_version()
        # Should be a non-empty version string when marimo is installed
        assert result
        assert result != "0.0.0"

    def test_returns_fallback_when_not_installed(self) -> None:
        from importlib.metadata import PackageNotFoundError

        with patch(
            "marimo_dagster._marimo_ast.version",
            side_effect=PackageNotFoundError("marimo"),
        ):
            assert _marimo_version() == "0.0.0"
