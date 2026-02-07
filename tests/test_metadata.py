"""Tests for PEP 723 script metadata parsing and generation."""

from conftest import EXAMPLES_DIR
from marimo_dagster._ir import ScriptMetadata
from marimo_dagster._metadata import (
    generate_pep723_metadata,
    parse_pep723_metadata,
    transform_dependencies,
)


class TestParsePep723Metadata:
    """Tests for parse_pep723_metadata."""

    def test_parse_dagster_example(self) -> None:
        source = EXAMPLES_DIR.joinpath(
            "dagster/tier1/simple_asset.py"
        ).read_text()
        meta = parse_pep723_metadata(source)
        assert meta.requires_python == ">=3.12"
        assert "dagster>=1.9.0" in meta.dependencies

    def test_parse_marimo_example(self) -> None:
        source = EXAMPLES_DIR.joinpath(
            "marimo/tier1/querying_dataframes.py"
        ).read_text()
        meta = parse_pep723_metadata(source)
        assert meta.requires_python == ">=3.10"
        assert "marimo" in meta.dependencies
        assert "polars==1.18.0" in meta.dependencies

    def test_parse_no_metadata(self) -> None:
        source = "import os\nprint('hello')\n"
        meta = parse_pep723_metadata(source)
        assert meta.requires_python is None
        assert meta.dependencies == []

    def test_parse_empty_string(self) -> None:
        meta = parse_pep723_metadata("")
        assert meta.requires_python is None
        assert meta.dependencies == []

    def test_parse_metadata_with_multiple_deps(self) -> None:
        source = (
            '# /// script\n'
            '# requires-python = ">=3.12"\n'
            '# dependencies = [\n'
            '#     "dagster>=1.9.0",\n'
            '#     "polars>=1.0",\n'
            '#     "requests",\n'
            '# ]\n'
            '# ///\n'
        )
        meta = parse_pep723_metadata(source)
        assert meta.requires_python == ">=3.12"
        assert len(meta.dependencies) == 3
        assert "dagster>=1.9.0" in meta.dependencies
        assert "polars>=1.0" in meta.dependencies
        assert "requests" in meta.dependencies


class TestGeneratePep723Metadata:
    """Tests for generate_pep723_metadata."""

    def test_generate_with_deps(self) -> None:
        meta = ScriptMetadata(
            requires_python=">=3.12",
            dependencies=["dagster>=1.9.0"],
        )
        result = generate_pep723_metadata(meta)
        assert "# /// script" in result
        assert "# ///" in result
        assert '# requires-python = ">=3.12"' in result
        assert '#     "dagster>=1.9.0",' in result

    def test_generate_empty_metadata(self) -> None:
        meta = ScriptMetadata()
        result = generate_pep723_metadata(meta)
        assert result == ""

    def test_generate_multiple_deps(self) -> None:
        meta = ScriptMetadata(
            requires_python=">=3.12",
            dependencies=["dagster>=1.9.0", "polars>=1.0"],
        )
        result = generate_pep723_metadata(meta)
        assert '#     "dagster>=1.9.0",' in result
        assert '#     "polars>=1.0",' in result

    def test_generate_roundtrip(self) -> None:
        """Parsing generated metadata produces the same ScriptMetadata."""
        original = ScriptMetadata(
            requires_python=">=3.12",
            dependencies=["dagster>=1.9.0", "polars>=1.0"],
        )
        text = generate_pep723_metadata(original)
        parsed = parse_pep723_metadata(text)
        assert parsed.requires_python == original.requires_python
        assert parsed.dependencies == original.dependencies


class TestTransformDependencies:
    """Tests for transform_dependencies."""

    def test_marimo_to_dagster(self) -> None:
        deps = ["marimo", "polars==1.18.0", "duckdb==1.1.1"]
        result = transform_dependencies(deps, from_framework="marimo", to_framework="dagster")
        assert "dagster" in result
        assert "marimo" not in result
        assert "polars==1.18.0" in result
        assert "duckdb==1.1.1" in result

    def test_dagster_to_marimo(self) -> None:
        deps = ["dagster>=1.9.0"]
        result = transform_dependencies(deps, from_framework="dagster", to_framework="marimo")
        assert "marimo" in result
        assert "dagster>=1.9.0" not in result

    def test_no_framework_dep(self) -> None:
        """If the source framework isn't present, just add the target."""
        deps = ["polars>=1.0"]
        result = transform_dependencies(deps, from_framework="marimo", to_framework="dagster")
        assert "dagster" in result
        assert "polars>=1.0" in result

    def test_preserves_non_framework_deps(self) -> None:
        deps = ["dagster>=1.9.0", "requests", "pandas>=2.0"]
        result = transform_dependencies(deps, from_framework="dagster", to_framework="marimo")
        assert "requests" in result
        assert "pandas>=2.0" in result

    def test_versioned_framework_dep_stripped(self) -> None:
        """dagster>=1.9.0 should be recognized as the dagster framework."""
        deps = ["dagster>=1.9.0", "polars"]
        result = transform_dependencies(deps, from_framework="dagster", to_framework="marimo")
        assert not any(d.startswith("dagster") for d in result)
        assert "marimo" in result

    def test_target_already_present(self) -> None:
        """If target framework is already in deps, don't add duplicate."""
        deps = ["marimo", "polars"]
        result = transform_dependencies(deps, from_framework="dagster", to_framework="marimo")
        assert result.count("marimo") == 1


class TestParsePep723EdgeCases:
    """Edge cases for PEP 723 parsing."""

    def test_bare_hash_comment_line(self) -> None:
        """A line with just '#' (no space after) should be handled."""
        source = (
            '# /// script\n'
            '#\n'
            '# requires-python = ">=3.12"\n'
            '# ///\n'
        )
        meta = parse_pep723_metadata(source)
        assert meta.requires_python == ">=3.12"

    def test_generate_deps_only_no_python(self) -> None:
        """Generate with dependencies but no requires-python."""
        meta = ScriptMetadata(dependencies=["polars"])
        result = generate_pep723_metadata(meta)
        assert "# /// script" in result
        assert "polars" in result
        assert "requires-python" not in result

    def test_generate_python_only_no_deps(self) -> None:
        """Generate with requires-python but no dependencies."""
        meta = ScriptMetadata(requires_python=">=3.12")
        result = generate_pep723_metadata(meta)
        assert "# /// script" in result
        assert ">=3.12" in result
        assert "dependencies" not in result
