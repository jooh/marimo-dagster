"""Tests for marimo-dagster converter functions."""

import ast
from pathlib import Path

from marimo_dagster.converter import dagster_to_marimo, marimo_to_dagster


class TestMarimoToDagster:
    """Tests for marimo_to_dagster conversion."""

    def test_marimo_to_dagster_returns_string(self) -> None:
        """Verify marimo_to_dagster returns a string."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        assert isinstance(result, str)

    def test_marimo_to_dagster_produces_valid_python(self) -> None:
        """Verify output is valid Python."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        ast.parse(result)

    def test_marimo_to_dagster_with_example(
        self, marimo_tier1_example: Path
    ) -> None:
        """Test marimo_to_dagster with tier 1 examples."""
        source = marimo_tier1_example.read_text()
        result = marimo_to_dagster(source)
        # Verify valid Python
        ast.parse(result)
        # Verify dagster structure
        assert "import dagster as dg" in result


class TestDagsterToMarimo:
    """Tests for dagster_to_marimo conversion."""

    def test_dagster_to_marimo_returns_string(self) -> None:
        """Verify dagster_to_marimo returns a string."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        result = dagster_to_marimo(source)
        assert isinstance(result, str)

    def test_dagster_to_marimo_produces_valid_python(self) -> None:
        """Verify output is valid Python."""
        source = (
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        result = dagster_to_marimo(source)
        ast.parse(result)  # Should not raise

    def test_dagster_to_marimo_with_example(
        self, dagster_tier1_example: Path
    ) -> None:
        """Test dagster_to_marimo with tier 1 examples."""
        source = dagster_tier1_example.read_text()
        result = dagster_to_marimo(source)
        # Verify valid Python
        ast.parse(result)
        # Verify marimo structure
        assert "import marimo" in result
        assert "@app.cell" in result
        assert "app.run()" in result


class TestMetadataTransformation:
    """Tests that PEP 723 dependencies are rewritten during conversion."""

    def test_dagster_to_marimo_rewrites_deps(self) -> None:
        """dagster_to_marimo should replace dagster dep with marimo in PEP 723."""
        source = (
            '# /// script\n'
            '# requires-python = ">=3.12"\n'
            '# dependencies = [\n'
            '#     "dagster>=1.9.0",\n'
            '#     "polars>=1.0",\n'
            '# ]\n'
            '# ///\n'
            'import dagster as dg\n'
            '\n'
            '@dg.asset\n'
            'def my_data() -> dict:\n'
            '    return {"x": 1}\n'
        )
        result = dagster_to_marimo(source)
        assert '"marimo"' in result
        assert "dagster" not in result.split("# ///")[1].split("# ///")[0]

    def test_marimo_to_dagster_rewrites_deps(self) -> None:
        """marimo_to_dagster should replace marimo dep with dagster in PEP 723."""
        source = (
            '# /// script\n'
            '# requires-python = ">=3.12"\n'
            '# dependencies = [\n'
            '#     "marimo",\n'
            '#     "polars>=1.0",\n'
            '# ]\n'
            '# ///\n'
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        assert '"dagster"' in result
        # marimo should not appear in PEP 723 metadata section
        assert "marimo" not in result.split("# ///")[1].split("# ///")[0]


class TestRoundTrip:
    """Tests for round-trip conversion (structural equivalence).

    These tests verify that:
    - dagster -> marimo -> dagster preserves asset structure and dependencies
    """

    def test_roundtrip_dagster_to_marimo_to_dagster(
        self, dagster_tier1_example: Path
    ) -> None:
        """Test dagster -> marimo -> dagster round-trip preserves assets."""
        from marimo_dagster._dagster_ast import parse_dagster

        original_source = dagster_tier1_example.read_text()

        # Round-trip
        marimo_source = dagster_to_marimo(original_source)
        roundtrip_source = marimo_to_dagster(marimo_source)

        # Valid Python
        ast.parse(roundtrip_source)

        # Verify asset count preserved
        original_ir = parse_dagster(original_source)
        roundtrip_ir = parse_dagster(roundtrip_source)
        assert len(roundtrip_ir.cells) == len(original_ir.cells)

        # Verify asset names preserved
        original_names = [c.name for c in original_ir.cells]
        roundtrip_names = [c.name for c in roundtrip_ir.cells]
        assert roundtrip_names == original_names

        # Verify dependency graph preserved
        for orig, rt in zip(original_ir.cells, roundtrip_ir.cells):
            assert set(rt.inputs) == set(orig.inputs)


class TestAllExamplesConversion:
    """Tests that all examples across all tiers produce valid output."""

    def test_dagster_to_marimo_all_examples(
        self, dagster_all_example: Path
    ) -> None:
        """Every dagster example should produce valid marimo Python."""
        source = dagster_all_example.read_text()
        result = dagster_to_marimo(source)
        ast.parse(result)
        assert "import marimo" in result
        assert "@app.cell" in result

    def test_marimo_to_dagster_all_examples(
        self, marimo_all_example: Path
    ) -> None:
        """Every marimo example should produce valid dagster Python."""
        source = marimo_all_example.read_text()
        result = marimo_to_dagster(source)
        ast.parse(result)
        assert "import dagster as dg" in result


class TestDagsterConversionPreservesAssets:
    """Tests that dagster → marimo conversion preserves asset structure."""

    def test_dagster_to_marimo_preserves_asset_count(
        self, dagster_all_example: Path
    ) -> None:
        """Number of @dg.asset functions should match number of code cells."""
        from marimo_dagster._dagster_ast import parse_dagster
        from marimo_dagster._marimo_ast import parse_marimo
        from marimo_dagster._ir import CellType

        original_ir = parse_dagster(dagster_all_example.read_text())
        result = dagster_to_marimo(dagster_all_example.read_text())
        marimo_ir = parse_marimo(result)

        # Each dagster asset becomes a CODE cell in marimo (plus docstring
        # markdown cells). Count CODE cells only.
        code_cells = [c for c in marimo_ir.cells if c.cell_type == CellType.CODE]
        assert len(code_cells) == len(original_ir.cells)


class TestMarimoConversionCellSurvival:
    """Tests documenting which marimo cells survive dagster conversion.

    CODE and SQL cells are converted to dagster assets. MARKDOWN, UI,
    and DISPLAY_ONLY cells are dropped.
    """

    def test_code_and_sql_cells_survive(
        self, marimo_all_example: Path
    ) -> None:
        """CODE and SQL cells from marimo should become dagster assets."""
        from marimo_dagster._dagster_ast import parse_dagster
        from marimo_dagster._marimo_ast import parse_marimo
        from marimo_dagster._ir import CellType

        source = marimo_all_example.read_text()
        marimo_ir = parse_marimo(source)
        dagster_source = marimo_to_dagster(source)
        dagster_ir = parse_dagster(dagster_source)

        convertible = [
            c for c in marimo_ir.cells
            if c.cell_type in (CellType.CODE, CellType.SQL)
        ]
        assert len(dagster_ir.cells) == len(convertible)


class TestSqlCellConversion:
    """Tests that SQL cells are converted to duckdb.sql() in dagster output."""

    def test_sql_cell_becomes_dagster_asset(self) -> None:
        """A marimo SQL cell should produce a dagster asset using duckdb.sql."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    result = mo.sql(f"SELECT 1 as x")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        ast.parse(result)
        assert "duckdb.sql" in result
        assert "mo.sql" not in result
        assert "@dg.asset" in result

    def test_duckdb_import_added(self) -> None:
        """duckdb should be added to imports when SQL cells exist."""
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
        result = marimo_to_dagster(source)
        assert "import duckdb" in result

    def test_sql_cells_add_duckdb_and_polars_deps(self) -> None:
        """duckdb and polars should be added to script dependencies for SQL cells."""
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
        result = marimo_to_dagster(source)
        assert '"duckdb"' in result
        assert '"polars"' in result

    def test_sql_cells_no_duplicate_deps(self) -> None:
        """Existing duckdb/polars deps should not be duplicated."""
        source = (
            '# /// script\n'
            '# dependencies = [\n'
            '#     "marimo",\n'
            '#     "duckdb==1.2.2",\n'
            '#     "polars[pyarrow]==1.27.1",\n'
            '# ]\n'
            '# ///\n'
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    result = mo.sql(f"SELECT 1")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        # The deps block should keep originals, not add bare duplicates
        assert '"duckdb==1.2.2"' in result
        assert '"polars[pyarrow]==1.27.1"' in result
        # No bare "duckdb" or "polars" dep lines (the versioned ones suffice)
        assert '"duckdb",' not in result
        assert '"polars",' not in result

    def test_duckdb_example_converts_sql_cells(self) -> None:
        """duckdb_example.py SQL cells should survive as dagster assets."""
        from pathlib import Path as P

        source = P("tests/examples/marimo/tier2/duckdb_example.py").read_text()
        result = marimo_to_dagster(source)
        ast.parse(result)
        assert "duckdb.sql" in result
        # All 3 cells should survive (2 CODE + 1 SQL)
        assert result.count("@dg.asset") == 3

    def test_sql_output_kwarg_stripped(self) -> None:
        """marimo-specific kwargs like output=False should be stripped."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    result = mo.sql(f"SELECT 1", output=False)\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        assert "output=False" not in result
        assert "duckdb.sql" in result

    def test_no_sql_cells_no_duckdb_import(self) -> None:
        """When no SQL cells exist, duckdb should not be added."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    x = 1\n'
            '    return (x,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        assert "duckdb" not in result

    def test_duckdb_import_not_duplicated(self) -> None:
        """If duckdb is already imported, don't add it again."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    import marimo as mo\n'
            '    import duckdb\n'
            '    return (mo, duckdb)\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    result = mo.sql(f"SELECT 1")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        # Should have duckdb import only once (from the original, not duplicated)
        assert result.count("import duckdb") == 1

    def test_aliased_duckdb_import_gets_unaliased_import(self) -> None:
        """If duckdb is imported with alias, an unaliased import must also exist."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _():\n'
            '    import duckdb as db\n'
            '    return (db,)\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    result = mo.sql(f"SELECT 1")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        ast.parse(result)
        assert "duckdb.sql" in result
        # The rewritten code uses `duckdb.sql(...)` so an unaliased import
        # must be present (not just `import duckdb as db`).
        assert "import duckdb\n" in result

    def test_sql_rewrite_returns_polars_dataframe(self) -> None:
        """mo.sql() returns a DataFrame; duckdb.sql() must call .pl() to match."""
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
        result = marimo_to_dagster(source)
        assert "duckdb.sql(" in result
        assert ".pl()" in result

    def test_non_mo_sql_calls_not_rewritten(self) -> None:
        """Other function calls in SQL cells should not be rewritten."""
        source = (
            'import marimo\n'
            'app = marimo.App()\n'
            '@app.cell\n'
            'def _(mo):\n'
            '    print("before")\n'
            '    result = mo.sql(f"SELECT 1")\n'
            '    return (result,)\n'
            'if __name__ == "__main__":\n'
            '    app.run()\n'
        )
        result = marimo_to_dagster(source)
        assert "print" in result
        assert "duckdb.sql" in result


class TestDagsterRoundTripAllTiers:
    """Round-trip tests for all dagster examples including tier 2/3."""

    def test_roundtrip_preserves_asset_names(
        self, dagster_all_example: Path
    ) -> None:
        """dagster -> marimo -> dagster should preserve asset names."""
        from marimo_dagster._dagster_ast import parse_dagster

        original_source = dagster_all_example.read_text()
        marimo_source = dagster_to_marimo(original_source)
        roundtrip_source = marimo_to_dagster(marimo_source)

        ast.parse(roundtrip_source)

        original_ir = parse_dagster(original_source)
        roundtrip_ir = parse_dagster(roundtrip_source)

        original_names = [c.name for c in original_ir.cells]
        roundtrip_names = [c.name for c in roundtrip_ir.cells]
        assert roundtrip_names == original_names
