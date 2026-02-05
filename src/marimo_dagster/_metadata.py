"""PEP 723 script metadata parsing and generation."""

import re
import tomllib

from marimo_dagster._ir import ScriptMetadata

_PEP723_PATTERN = re.compile(
    r"^# /// script\s*\n((?:#[^\n]*\n)*?)# ///$",
    re.MULTILINE,
)


def parse_pep723_metadata(source: str) -> ScriptMetadata:
    """Extract PEP 723 script metadata from source text."""
    match = _PEP723_PATTERN.search(source)
    if not match:
        return ScriptMetadata()

    # Strip leading "# " from each line to get raw TOML
    toml_lines = []
    for line in match.group(1).splitlines():
        # Remove "# " prefix (or just "#" for blank comment lines)
        if line.startswith("# "):
            toml_lines.append(line[2:])
        else:
            # Bare "#" line (no space after)
            toml_lines.append(line[1:])
    toml_text = "\n".join(toml_lines)

    data = tomllib.loads(toml_text)
    return ScriptMetadata(
        requires_python=data.get("requires-python"),
        dependencies=data.get("dependencies", []),
    )


def generate_pep723_metadata(metadata: ScriptMetadata) -> str:
    """Generate PEP 723 script metadata block.

    Returns empty string if metadata has no content.
    """
    if metadata.requires_python is None and not metadata.dependencies:
        return ""

    lines = ["# /// script"]
    if metadata.requires_python is not None:
        lines.append(f'# requires-python = "{metadata.requires_python}"')
    if metadata.dependencies:
        lines.append("# dependencies = [")
        for dep in metadata.dependencies:
            lines.append(f'#     "{dep}",')
        lines.append("# ]")
    lines.append("# ///")
    return "\n".join(lines) + "\n"


def transform_dependencies(
    deps: list[str],
    *,
    from_framework: str,
    to_framework: str,
) -> list[str]:
    """Swap framework dependency while preserving others."""
    result = []
    for dep in deps:
        # Extract bare package name (before any version specifier)
        bare_name = re.split(r"[><=!~\[]", dep)[0].strip()
        if bare_name != from_framework:
            result.append(dep)

    # Always ensure the target framework is present
    if not any(re.split(r"[><=!~\[]", d)[0].strip() == to_framework for d in result):
        result.insert(0, to_framework)

    return result
