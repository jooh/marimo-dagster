"""Tests for marimo-dagster converter functions."""

import ast
from pathlib import Path

import pytest

from marimo_dagster.converter import (
    _is_asset_decorator,
    dagster_to_marimo,
    marimo_to_dagster,
)


class TestMarimoToDagster:
    """Tests for marimo_to_dagster conversion."""

    def test_marimo_to_dagster_raises_not_implemented(self) -> None:
        """Verify marimo_to_dagster raises NotImplementedError (stub)."""
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            marimo_to_dagster("# some marimo code")

    def test_marimo_to_dagster_with_example(
        self, marimo_tier1_example: Path
    ) -> None:
        """Test marimo_to_dagster with tier 1 examples."""
        source = marimo_tier1_example.read_text()
        # Should raise NotImplementedError until implemented
        with pytest.raises(NotImplementedError):
            marimo_to_dagster(source)


def _count_dagster_assets(source: str) -> int:
    """Count the number of @dg.asset decorated functions in dagster source."""
    tree = ast.parse(source)
    count = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for decorator in node.decorator_list:
                # Handle @dg.asset
                if isinstance(decorator, ast.Attribute) and decorator.attr == "asset":
                    count += 1
                # Handle @dg.asset(...)
                elif isinstance(decorator, ast.Call):
                    func = decorator.func
                    if isinstance(func, ast.Attribute) and func.attr == "asset":
                        count += 1
    return count


def _assert_valid_marimo_output(source: str, result: str) -> None:
    """Assert that converted marimo output is valid."""
    # Must be valid Python
    ast.parse(result)

    # Must have marimo import and app creation
    assert "import marimo" in result
    assert "app = marimo.App()" in result

    # Must have at least one @app.cell decorator
    assert "@app.cell" in result

    # Must have the if __name__ block at the end
    assert 'if __name__ == "__main__"' in result
    assert "app.run()" in result

    # Each asset should have a comment marker
    asset_count = _count_dagster_assets(source)
    assert result.count("# asset:") == asset_count


class TestDagsterToMarimo:
    """Tests for dagster_to_marimo conversion."""

    def test_dagster_to_marimo_tier1(
        self, dagster_tier1_example: Path
    ) -> None:
        """Test dagster_to_marimo with tier 1 examples produces valid marimo."""
        source = dagster_tier1_example.read_text()
        result = dagster_to_marimo(source)
        _assert_valid_marimo_output(source, result)

    def test_dagster_to_marimo_tier2(
        self, dagster_tier2_example: Path
    ) -> None:
        """Test dagster_to_marimo with tier 2 examples produces valid marimo."""
        source = dagster_tier2_example.read_text()
        result = dagster_to_marimo(source)
        _assert_valid_marimo_output(source, result)

    def test_dagster_to_marimo_no_pep723_metadata(self) -> None:
        """Test conversion of dagster source without PEP 723 metadata."""
        source = '''
import dagster as dg

@dg.asset
def my_asset() -> dict:
    return {"value": 42}
'''
        result = dagster_to_marimo(source)

        # Should still produce valid marimo
        ast.parse(result)
        assert "import marimo" in result
        assert "@app.cell" in result
        assert "# asset: my_asset" in result
        # Should NOT have PEP 723 metadata block
        assert "# /// script" not in result

    def test_dagster_to_marimo_asset_without_return(self) -> None:
        """Test conversion of asset that doesn't return anything."""
        source = '''
import dagster as dg

@dg.asset
def side_effect_asset() -> None:
    print("side effect")
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        assert "# asset: side_effect_asset" in result
        # Should assign None when no return
        assert "side_effect_asset = None" in result
        assert "return (side_effect_asset,)" in result

    def test_dagster_to_marimo_asset_without_docstring(self) -> None:
        """Test conversion of asset without docstring."""
        source = '''
import dagster as dg

@dg.asset
def no_docstring() -> dict:
    return {"x": 1}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        assert "# asset: no_docstring" in result
        # Should not have extra comment lines beyond asset marker
        lines = [line for line in result.split("\n") if line.strip().startswith("# ")]
        # Only PEP 723 lines and asset marker should be comments
        asset_comments = [line for line in lines if "asset:" in line]
        assert len(asset_comments) == 1

    def test_dagster_to_marimo_with_non_asset_functions(self) -> None:
        """Test that non-asset functions are ignored."""
        source = '''
import dagster as dg

def helper_function():
    return 42

@dg.asset
def my_asset() -> dict:
    return {"value": helper_function()}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        # Should only have one asset marker
        assert result.count("# asset:") == 1
        assert "# asset: my_asset" in result
        # Helper function should not appear as a cell
        assert "# asset: helper_function" not in result

    def test_dagster_to_marimo_asset_with_decorator_args(self) -> None:
        """Test conversion of asset with decorator arguments like @dg.asset(group=...)."""
        source = '''
import dagster as dg

@dg.asset(group="my_group")
def grouped_asset() -> dict:
    return {"value": 1}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        assert "# asset: grouped_asset" in result
        assert "@app.cell" in result

    def test_dagster_to_marimo_handles_external_deps(self) -> None:
        """Test that assets with deps not in the module are handled gracefully."""
        # This tests the case where an asset depends on something not defined in the file
        source = '''
import dagster as dg

@dg.asset
def downstream(external_dep: dict) -> dict:
    return {"processed": external_dep["value"]}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        assert "# asset: downstream" in result
        # Should still have the dependency in function params
        assert "def _(external_dep):" in result

    def test_dagster_to_marimo_ignores_other_call_decorators(self) -> None:
        """Test that non-asset Call decorators are ignored."""
        source = '''
import dagster as dg

def my_decorator(func):
    return func

@my_decorator()
def decorated_func():
    return 1

@dg.asset
def my_asset() -> dict:
    return {"value": decorated_func()}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        # Should only have one asset marker
        assert result.count("# asset:") == 1
        assert "# asset: my_asset" in result
        # The decorated_func should not appear as an asset
        assert "# asset: decorated_func" not in result


class TestIsAssetDecorator:
    """Direct tests for _is_asset_decorator function."""

    def test_call_decorator_with_name_func(self) -> None:
        """Test that @some_func() decorators return False."""
        # This covers the branch where decorator is Call but func is Name (not Attribute)
        code = "@some_func()\ndef f(): pass"
        tree = ast.parse(code)
        func_def = tree.body[0]
        decorator = func_def.decorator_list[0]  # type: ignore[attr-defined]

        # decorator is ast.Call, decorator.func is ast.Name
        assert isinstance(decorator, ast.Call)
        assert isinstance(decorator.func, ast.Name)
        assert _is_asset_decorator(decorator) is False

    def test_name_decorator_returns_false(self) -> None:
        """Test that @plain_decorator (Name, not Call or Attribute) returns False."""
        # This covers the branch where decorator is neither Attribute nor Call
        code = "@plain_decorator\ndef f(): pass"
        tree = ast.parse(code)
        func_def = tree.body[0]
        decorator = func_def.decorator_list[0]  # type: ignore[attr-defined]

        # decorator is ast.Name (not Attribute or Call)
        assert isinstance(decorator, ast.Name)
        assert _is_asset_decorator(decorator) is False

    def test_direct_asset_name_decorator(self) -> None:
        """Test that @asset (direct import) is recognized."""
        # When using: from dagster import asset
        code = "@asset\ndef f(): pass"
        tree = ast.parse(code)
        func_def = tree.body[0]
        decorator = func_def.decorator_list[0]  # type: ignore[attr-defined]

        assert isinstance(decorator, ast.Name)
        assert decorator.id == "asset"
        assert _is_asset_decorator(decorator) is True

    def test_direct_asset_call_decorator(self) -> None:
        """Test that @asset(...) (direct import with args) is recognized."""
        # When using: from dagster import asset; @asset(group="foo")
        code = "@asset(group='foo')\ndef f(): pass"
        tree = ast.parse(code)
        func_def = tree.body[0]
        decorator = func_def.decorator_list[0]  # type: ignore[attr-defined]

        assert isinstance(decorator, ast.Call)
        assert isinstance(decorator.func, ast.Name)
        assert decorator.func.id == "asset"
        assert _is_asset_decorator(decorator) is True

    def test_complex_call_decorator_returns_false(self) -> None:
        """Test that complex call decorators (e.g., @foo.bar()()) return False."""
        # This is an edge case: decorator is Call but func is also a Call
        code = "@get_decorator()('arg')\ndef f(): pass"
        tree = ast.parse(code)
        func_def = tree.body[0]
        decorator = func_def.decorator_list[0]  # type: ignore[attr-defined]

        # decorator is ast.Call, decorator.func is also ast.Call (not Name or Attribute)
        assert isinstance(decorator, ast.Call)
        assert isinstance(decorator.func, ast.Call)
        assert _is_asset_decorator(decorator) is False

    def test_subscript_decorator_returns_false(self) -> None:
        """Test that subscript decorators (e.g., @decorators[0]) return False."""
        # This covers the case where decorator is neither Attribute, Name, nor Call
        code = "@decorators[0]\ndef f(): pass"
        tree = ast.parse(code)
        func_def = tree.body[0]
        decorator = func_def.decorator_list[0]  # type: ignore[attr-defined]

        # decorator is ast.Subscript (not Attribute, Name, or Call)
        assert isinstance(decorator, ast.Subscript)
        assert _is_asset_decorator(decorator) is False


class TestDirectAssetImport:
    """Tests for assets using direct import: from dagster import asset."""

    def test_dagster_to_marimo_direct_asset_import(self) -> None:
        """Test conversion when asset is imported directly."""
        source = '''
from dagster import asset

@asset
def my_asset() -> dict:
    return {"value": 42}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        assert "# asset: my_asset" in result
        assert "@app.cell" in result

    def test_dagster_to_marimo_direct_asset_with_args(self) -> None:
        """Test conversion when asset is imported directly and has decorator args."""
        source = '''
from dagster import asset

@asset(group="my_group")
def grouped_asset() -> dict:
    return {"value": 1}
'''
        result = dagster_to_marimo(source)

        ast.parse(result)
        assert "# asset: grouped_asset" in result
        assert "@app.cell" in result


class TestRoundTrip:
    """Tests for round-trip conversion (functional equivalence).

    These tests verify that:
    - marimo -> dagster -> marimo produces functionally equivalent output
    - dagster -> marimo -> dagster produces functionally equivalent output

    For now, these are marked as expected to fail until converters are implemented.
    """

    @pytest.mark.xfail(reason="Converters not yet implemented", raises=NotImplementedError)
    def test_roundtrip_marimo_to_dagster_to_marimo(
        self, marimo_tier1_example: Path, example_venv_runner
    ) -> None:
        """Test marimo -> dagster -> marimo round-trip produces equivalent output.

        Functional equivalence is tested by running both the original and
        round-tripped notebooks and comparing their outputs.
        """
        original_source = marimo_tier1_example.read_text()

        # Convert marimo -> dagster -> marimo
        dagster_source = marimo_to_dagster(original_source)
        roundtrip_source = dagster_to_marimo(dagster_source)

        # TODO: Run both in isolated venvs and compare outputs
        # For now, just verify the conversions complete
        assert roundtrip_source is not None

    @pytest.mark.xfail(reason="Converters not yet implemented", raises=NotImplementedError)
    def test_roundtrip_dagster_to_marimo_to_dagster(
        self, dagster_tier1_example: Path, example_venv_runner
    ) -> None:
        """Test dagster -> marimo -> dagster round-trip produces equivalent output.

        Functional equivalence is tested by running both the original and
        round-tripped dagster assets and comparing their outputs.
        """
        original_source = dagster_tier1_example.read_text()

        # Convert dagster -> marimo -> dagster
        marimo_source = dagster_to_marimo(original_source)
        roundtrip_source = marimo_to_dagster(marimo_source)

        # TODO: Run both in isolated venvs and compare outputs
        # For now, just verify the conversions complete
        assert roundtrip_source is not None
