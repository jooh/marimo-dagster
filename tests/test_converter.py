"""Tests for marimo-dagster converter functions."""

from pathlib import Path

import pytest

from marimo_dagster.converter import dagster_to_marimo, marimo_to_dagster


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


class TestDagsterToMarimo:
    """Tests for dagster_to_marimo conversion."""

    def test_dagster_to_marimo_raises_not_implemented(self) -> None:
        """Verify dagster_to_marimo raises NotImplementedError (stub)."""
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            dagster_to_marimo("# some dagster code")

    def test_dagster_to_marimo_with_example(
        self, dagster_tier1_example: Path
    ) -> None:
        """Test dagster_to_marimo with tier 1 examples."""
        source = dagster_tier1_example.read_text()
        # Should raise NotImplementedError until implemented
        with pytest.raises(NotImplementedError):
            dagster_to_marimo(source)


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
