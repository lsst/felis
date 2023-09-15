# This file is part of felis.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = [
    "CheckConstraint",
    "Column",
    "Constraint",
    "ForeignKeyConstraint",
    "Index",
    "Schema",
    "SchemaVersion",
    "SimpleVisitor",
    "Table",
    "UniqueConstraint",
]

import dataclasses
import logging
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any, cast

from .check import FelisValidator
from .types import FelisType
from .visitor import Visitor

_Mapping = Mapping[str, Any]

logger = logging.getLogger("felis.generic")


def _strip_keys(map: _Mapping, keys: Iterable[str]) -> _Mapping:
    """Return a copy of a dictionary with some keys removed."""
    keys = set(keys)
    return {key: value for key, value in map.items() if key not in keys}


def _make_iterable(obj: str | Iterable[str]) -> Iterable[str]:
    """Make an iterable out of string or list of strings."""
    if isinstance(obj, str):
        yield obj
    else:
        yield from obj


@dataclasses.dataclass
class Column:
    """Column representation in schema."""

    name: str
    """Column name."""

    id: str
    """Felis ID for this column."""

    datatype: type[FelisType]
    """Column type, one of the types/classes defined in `types`."""

    length: int | None = None
    """Optional length for string/binary columns"""

    nullable: bool = True
    """True for nullable columns."""

    value: Any = None
    """Default value for column, can be `None`."""

    autoincrement: bool | None = None
    """Unspecified value results in `None`."""

    description: str | None = None
    """Column description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this column."""

    table: Table | None = None
    """Table which defines this column, usually not `None`."""


@dataclasses.dataclass
class Index:
    """Index representation."""

    name: str
    """index name, can be empty."""

    id: str
    """Felis ID for this index."""

    columns: list[Column] = dataclasses.field(default_factory=list)
    """List of columns in index, one of the ``columns`` or ``expressions``
    must be non-empty.
    """

    expressions: list[str] = dataclasses.field(default_factory=list)
    """List of expressions in index, one of the ``columns`` or ``expressions``
    must be non-empty.
    """

    description: str | None = None
    """Index description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this index."""


@dataclasses.dataclass
class Constraint:
    """Constraint description, this is a base class, actual constraints will be
    instances of one of the subclasses.
    """

    name: str | None
    """Constraint name."""

    id: str
    """Felis ID for this constraint."""

    deferrable: bool = False
    """If `True` then this constraint will be declared as deferrable."""

    initially: str | None = None
    """Value for ``INITIALLY`` clause, only used of ``deferrable`` is True."""

    description: str | None = None
    """Constraint description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this constraint."""


@dataclasses.dataclass
class UniqueConstraint(Constraint):
    """Description of unique constraint."""

    columns: list[Column] = dataclasses.field(default_factory=list)
    """List of columns in this constraint, all columns belong to the same table
    as the constraint itself.
    """


@dataclasses.dataclass
class ForeignKeyConstraint(Constraint):
    """Description of foreign key constraint."""

    columns: list[Column] = dataclasses.field(default_factory=list)
    """List of columns in this constraint, all columns belong to the same table
    as the constraint itself.
    """

    referenced_columns: list[Column] = dataclasses.field(default_factory=list)
    """List of referenced columns, the number of columns must be the same as in
    ``Constraint.columns`` list. All columns must belong to the same table,
    which is different from the table of this constraint.
    """


@dataclasses.dataclass
class CheckConstraint(Constraint):
    """Description of check constraint."""

    expression: str = ""
    """Expression on one or more columns on the table, must be non-empty."""


@dataclasses.dataclass
class Table:
    """Description of a single table schema."""

    name: str
    """Table name."""

    id: str
    """Felis ID for this table."""

    columns: list[Column]
    """List of Column instances."""

    primary_key: list[Column]
    """List of Column that constitute a primary key, may be empty."""

    constraints: list[Constraint]
    """List of Constraint instances, can be empty."""

    indexes: list[Index]
    """List of Index instances, can be empty."""

    description: str | None = None
    """Table description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this table."""


@dataclasses.dataclass
class SchemaVersion:
    """Schema versioning description."""

    current: str
    """Current schema version defined by the document."""

    compatible: list[str] | None = None
    """Optional list of versions which are compatible with current version."""

    read_compatible: list[str] | None = None
    """Optional list of versions with which current version is read-compatible.
    """


@dataclasses.dataclass
class Schema:
    """Complete schema description, collection of tables."""

    name: str
    """Schema name."""

    id: str
    """Felis ID for this schema."""

    tables: list[Table]
    """Collection of table definitions."""

    version: SchemaVersion | None = None
    """Schema version description."""

    description: str | None = None
    """Schema description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this table."""


class SimpleVisitor(Visitor[Schema, Table, Column, list[Column], Constraint, Index, SchemaVersion]):
    """Visitor implementation class that produces a simple in-memory
    representation of Felis schema using classes `Schema`, `Table`, etc. from
    this module.

    Notes
    -----
    Implementation of this visitor class uses `FelisValidator` to validate the
    contents of the schema. All visit methods can raise the same exceptions as
    corresponding `FelisValidator` methods (usually `ValueError`).
    """

    def __init__(self) -> None:
        self.checker = FelisValidator()
        self.column_ids: MutableMapping[str, Column] = {}

    def visit_schema(self, schema_obj: _Mapping) -> Schema:
        # Docstring is inherited.
        self.checker.check_schema(schema_obj)

        version_obj = schema_obj.get("version")

        schema = Schema(
            name=schema_obj["name"],
            id=schema_obj["@id"],
            tables=[self.visit_table(t, schema_obj) for t in schema_obj["tables"]],
            version=self.visit_schema_version(version_obj, schema_obj) if version_obj is not None else None,
            description=schema_obj.get("description"),
            annotations=_strip_keys(schema_obj, ["name", "@id", "tables", "description"]),
        )
        return schema

    def visit_schema_version(
        self, version_obj: str | Mapping[str, Any], schema_obj: Mapping[str, Any]
    ) -> SchemaVersion:
        # Docstring is inherited.
        self.checker.check_schema_version(version_obj, schema_obj)

        if isinstance(version_obj, str):
            return SchemaVersion(current=version_obj)
        else:
            return SchemaVersion(
                current=cast(str, version_obj["current"]),
                compatible=version_obj.get("compatible"),
                read_compatible=version_obj.get("read_compatible"),
            )

    def visit_table(self, table_obj: _Mapping, schema_obj: _Mapping) -> Table:
        # Docstring is inherited.
        self.checker.check_table(table_obj, schema_obj)

        columns = [self.visit_column(c, table_obj) for c in table_obj["columns"]]
        table = Table(
            name=table_obj["name"],
            id=table_obj["@id"],
            columns=columns,
            primary_key=self.visit_primary_key(table_obj.get("primaryKey", []), table_obj),
            constraints=[self.visit_constraint(c, table_obj) for c in table_obj.get("constraints", [])],
            indexes=[self.visit_index(i, table_obj) for i in table_obj.get("indexes", [])],
            description=table_obj.get("description"),
            annotations=_strip_keys(
                table_obj, ["name", "@id", "columns", "primaryKey", "constraints", "indexes", "description"]
            ),
        )
        for column in columns:
            column.table = table
        return table

    def visit_column(self, column_obj: _Mapping, table_obj: _Mapping) -> Column:
        # Docstring is inherited.
        self.checker.check_column(column_obj, table_obj)

        datatype = FelisType.felis_type(column_obj["datatype"])

        column = Column(
            name=column_obj["name"],
            id=column_obj["@id"],
            datatype=datatype,
            length=column_obj.get("length"),
            value=column_obj.get("value"),
            description=column_obj.get("description"),
            nullable=column_obj.get("nullable", True),
            autoincrement=column_obj.get("autoincrement"),
            annotations=_strip_keys(
                column_obj,
                ["name", "@id", "datatype", "length", "nullable", "value", "autoincrement", "description"],
            ),
        )
        if column.id in self.column_ids:
            logger.warning(f"Duplication of @id {column.id}")
        self.column_ids[column.id] = column
        return column

    def visit_primary_key(self, primary_key_obj: str | Iterable[str], table_obj: _Mapping) -> list[Column]:
        # Docstring is inherited.
        self.checker.check_primary_key(primary_key_obj, table_obj)
        if primary_key_obj:
            columns = [self.column_ids[c_id] for c_id in _make_iterable(primary_key_obj)]
            return columns
        return []

    def visit_constraint(self, constraint_obj: _Mapping, table_obj: _Mapping) -> Constraint:
        # Docstring is inherited.
        self.checker.check_constraint(constraint_obj, table_obj)

        constraint_type = constraint_obj["@type"]
        if constraint_type == "Unique":
            return UniqueConstraint(
                name=constraint_obj.get("name"),
                id=constraint_obj["@id"],
                columns=[self.column_ids[c_id] for c_id in _make_iterable(constraint_obj["columns"])],
                deferrable=constraint_obj.get("deferrable", False),
                initially=constraint_obj.get("initially"),
                description=constraint_obj.get("description"),
                annotations=_strip_keys(
                    constraint_obj,
                    ["name", "@type", "@id", "columns", "deferrable", "initially", "description"],
                ),
            )
        elif constraint_type == "ForeignKey":
            return ForeignKeyConstraint(
                name=constraint_obj.get("name"),
                id=constraint_obj["@id"],
                columns=[self.column_ids[c_id] for c_id in _make_iterable(constraint_obj["columns"])],
                referenced_columns=[
                    self.column_ids[c_id] for c_id in _make_iterable(constraint_obj["referencedColumns"])
                ],
                deferrable=constraint_obj.get("deferrable", False),
                initially=constraint_obj.get("initially"),
                description=constraint_obj.get("description"),
                annotations=_strip_keys(
                    constraint_obj,
                    [
                        "name",
                        "@id",
                        "@type",
                        "columns",
                        "deferrable",
                        "initially",
                        "referencedColumns",
                        "description",
                    ],
                ),
            )
        elif constraint_type == "Check":
            return CheckConstraint(
                name=constraint_obj.get("name"),
                id=constraint_obj["@id"],
                expression=constraint_obj["expression"],
                deferrable=constraint_obj.get("deferrable", False),
                initially=constraint_obj.get("initially"),
                description=constraint_obj.get("description"),
                annotations=_strip_keys(
                    constraint_obj,
                    ["name", "@id", "@type", "expression", "deferrable", "initially", "description"],
                ),
            )
        else:
            raise ValueError(f"Unexpected constrint type: {constraint_type}")

    def visit_index(self, index_obj: _Mapping, table_obj: _Mapping) -> Index:
        # Docstring is inherited.
        self.checker.check_index(index_obj, table_obj)

        return Index(
            name=index_obj["name"],
            id=index_obj["@id"],
            columns=[self.column_ids[c_id] for c_id in _make_iterable(index_obj.get("columns", []))],
            expressions=index_obj.get("expressions", []),
            description=index_obj.get("description"),
            annotations=_strip_keys(index_obj, ["name", "@id", "columns", "expressions", "description"]),
        )
