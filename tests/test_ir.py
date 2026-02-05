"""Tests for the intermediate representation dataclasses."""

import ast

from marimo_dagster._ir import CellNode, CellType, ImportItem, NotebookIR, ScriptMetadata


class TestCellType:
    """Tests for CellType enum."""

    def test_all_cell_types_exist(self) -> None:
        assert CellType.CODE.value == "code"
        assert CellType.IMPORT_ONLY.value == "import"
        assert CellType.MARKDOWN.value == "markdown"
        assert CellType.SQL.value == "sql"
        assert CellType.UI.value == "ui"
        assert CellType.DISPLAY_ONLY.value == "display"

    def test_cell_type_count(self) -> None:
        assert len(CellType) == 6


class TestImportItem:
    """Tests for ImportItem dataclass."""

    def test_plain_import(self) -> None:
        item = ImportItem(module="polars", alias="pl")
        assert item.module == "polars"
        assert item.alias == "pl"
        assert item.names is None

    def test_plain_import_no_alias(self) -> None:
        item = ImportItem(module="os")
        assert item.module == "os"
        assert item.alias is None
        assert item.names is None

    def test_from_import(self) -> None:
        item = ImportItem(module="vega_datasets", names=[("data", None)])
        assert item.module == "vega_datasets"
        assert item.names == [("data", None)]
        assert item.alias is None

    def test_from_import_with_alias(self) -> None:
        item = ImportItem(module="pathlib", names=[("Path", "P")])
        assert item.names == [("Path", "P")]

    def test_from_import_multiple_names(self) -> None:
        item = ImportItem(module="os.path", names=[("join", None), ("exists", None)])
        assert item.names is not None
        assert len(item.names) == 2


class TestCellNode:
    """Tests for CellNode dataclass."""

    def test_minimal_code_cell(self) -> None:
        stmts = ast.parse("x = 1").body
        cell = CellNode(
            name="my_cell",
            body_stmts=stmts,
            inputs=[],
            outputs=["x"],
        )
        assert cell.name == "my_cell"
        assert cell.inputs == []
        assert cell.outputs == ["x"]
        assert cell.cell_type == CellType.CODE
        assert cell.docstring is None
        assert cell.return_type_annotation is None

    def test_cell_with_dependencies(self) -> None:
        stmts = ast.parse("y = x + 1").body
        cell = CellNode(
            name="downstream",
            body_stmts=stmts,
            inputs=["x"],
            outputs=["y"],
            docstring="Computes y from x.",
        )
        assert cell.inputs == ["x"]
        assert cell.docstring == "Computes y from x."

    def test_cell_with_type_annotation(self) -> None:
        cell = CellNode(
            name="typed",
            body_stmts=[],
            inputs=[],
            outputs=["typed"],
            return_type_annotation="dict",
        )
        assert cell.return_type_annotation == "dict"

    def test_cell_with_explicit_type(self) -> None:
        cell = CellNode(
            name="md_cell",
            body_stmts=[],
            inputs=[],
            outputs=[],
            cell_type=CellType.MARKDOWN,
        )
        assert cell.cell_type == CellType.MARKDOWN

    def test_cell_body_stmts_are_ast_nodes(self) -> None:
        stmts = ast.parse("a = 1\nb = a + 2").body
        cell = CellNode(name="c", body_stmts=stmts, inputs=[], outputs=["a", "b"])
        assert len(cell.body_stmts) == 2
        assert isinstance(cell.body_stmts[0], ast.Assign)


class TestScriptMetadata:
    """Tests for ScriptMetadata dataclass."""

    def test_defaults(self) -> None:
        meta = ScriptMetadata()
        assert meta.requires_python is None
        assert meta.dependencies == []

    def test_with_values(self) -> None:
        meta = ScriptMetadata(
            requires_python=">=3.12",
            dependencies=["dagster>=1.9.0"],
        )
        assert meta.requires_python == ">=3.12"
        assert meta.dependencies == ["dagster>=1.9.0"]


class TestNotebookIR:
    """Tests for NotebookIR dataclass."""

    def test_defaults(self) -> None:
        ir = NotebookIR()
        assert ir.imports == []
        assert ir.cells == []
        assert ir.metadata == ScriptMetadata()
        assert ir.module_docstring is None

    def test_with_cells_and_imports(self) -> None:
        imp = ImportItem(module="polars", alias="pl")
        cell = CellNode(
            name="data",
            body_stmts=ast.parse("x = 1").body,
            inputs=[],
            outputs=["data"],
        )
        ir = NotebookIR(
            imports=[imp],
            cells=[cell],
            module_docstring="My notebook",
        )
        assert len(ir.imports) == 1
        assert len(ir.cells) == 1
        assert ir.module_docstring == "My notebook"

    def test_dependency_graph_through_names(self) -> None:
        """Verify the IR captures dependency relationships via name matching."""
        cell_a = CellNode(
            name="raw", body_stmts=[], inputs=[], outputs=["raw"]
        )
        cell_b = CellNode(
            name="processed", body_stmts=[], inputs=["raw"], outputs=["processed"]
        )
        ir = NotebookIR(cells=[cell_a, cell_b])
        # Dependency is implicit: cell_b.inputs references cell_a.outputs
        assert ir.cells[1].inputs[0] in ir.cells[0].outputs
