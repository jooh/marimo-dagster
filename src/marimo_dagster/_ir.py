"""Intermediate representation for notebook/asset conversion."""

import ast
from dataclasses import dataclass, field
from enum import Enum


class CellType(Enum):
    """Classification of a cell's purpose."""

    CODE = "code"
    IMPORT_ONLY = "import"
    MARKDOWN = "markdown"
    SQL = "sql"
    UI = "ui"
    DISPLAY_ONLY = "display"


@dataclass
class ImportItem:
    """A single import statement.

    Represents both ``import module`` and ``from module import name``.
    """

    module: str
    names: list[tuple[str, str | None]] | None = None
    alias: str | None = None

    def format_statement(self) -> str:
        """Format this import as a Python import statement."""
        if self.names is not None:
            parts = [f"{name} as {alias}" if alias else name for name, alias in self.names]
            return f"from {self.module} import {', '.join(parts)}"
        if self.alias:
            return f"import {self.module} as {self.alias}"
        return f"import {self.module}"

    def exported_names(self) -> list[str]:
        """Return the names this import makes available in the local scope."""
        if self.names is not None:
            return [alias or name for name, alias in self.names]
        return [self.alias or self.module]


@dataclass
class CellNode:
    """A single computational unit (marimo cell or dagster asset)."""

    name: str
    body_stmts: list[ast.stmt]
    inputs: list[str]
    outputs: list[str]
    cell_type: CellType = CellType.CODE
    docstring: str | None = None
    return_type_annotation: str | None = None
    decorator_kwargs: dict[str, str] = field(default_factory=dict)


@dataclass
class ScriptMetadata:
    """PEP 723 script metadata."""

    requires_python: str | None = None
    dependencies: list[str] = field(default_factory=list)


@dataclass
class NotebookIR:
    """Complete intermediate representation of a notebook or asset module."""

    imports: list[ImportItem] = field(default_factory=list)
    cells: list[CellNode] = field(default_factory=list)
    metadata: ScriptMetadata = field(default_factory=ScriptMetadata)
    module_docstring: str | None = None
