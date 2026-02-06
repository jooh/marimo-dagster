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
