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
