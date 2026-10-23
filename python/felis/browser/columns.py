"""The columns table of a table's page: the fields shown and the tags after
a column's name.
"""

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

from typing import NamedTuple

from felis.datamodel import Column, Schema, Table

from .schemas import _foreign_keys, _primary_key_columns, _resolve_column

_COLUMN_FIELDS = (
    "name",
    "datatype",
    "description",
    "unit",
    "nullable",
    "ivoa:ucd",
    "tap:column_index",
    "tap:principal",
    "tap:std",
    "votable:arraysize",
    "votable:datatype",
    "votable:xtype",
    "votable:utype",
    "value",
    "length",
    "precision",
    "autoincrement",
)
"""Column fields shown in the columns table, in order, by their names in the
schema YAML, except ``unit``: a column's ``ivoa:unit`` or ``fits:tunit``,
which are never both set.
"""


_REQUIRED_COLUMN_FIELDS = frozenset({"name", "datatype", "description", "unit", "nullable"})
"""Column fields always shown in the columns table, even if empty."""


_COLUMN_ATTRIBUTES = {field.alias or name: name for name, field in Column.model_fields.items()}
"""Column model attribute names, keyed by schema YAML field name."""


def _format_field_value(value: object) -> str:
    """Format a field value for display: "" for `None`, "true"/"false", lists
    comma-separated.
    """
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return str(value)


def _column_value(column: Column, field: str) -> object:
    """Return a column's value for a YAML field name, or for "unit" its
    ``ivoa:unit`` or ``fits:tunit``.
    """
    if field == "unit":
        return column.ivoa_unit or column.fits_tunit
    return getattr(column, _COLUMN_ATTRIBUTES[field])


def _field_default(field: str) -> object:
    """Return a field's default: `None` for most, 0 for ``tap:principal`` and
    ``tap:std``.
    """
    if field == "unit":
        return None
    return Column.model_fields[_COLUMN_ATTRIBUTES[field]].get_default(call_default_factory=True)


def _column_field(column: Column, field: str) -> str:
    return _format_field_value(_column_value(column, field))


def _column_sets_field(column: Column, field: str) -> bool:
    """Return whether a column sets a field to a non-empty value other than its
    default.
    """
    value = _column_value(column, field)
    return _format_field_value(value) != "" and value != _field_default(field)


def _table_column_fields(table: Table, show_empty: bool) -> list[str]:
    """Return the fields to show in a table's columns table, in
    `_COLUMN_FIELDS` order.

    Unless ``show_empty`` is true, a field is left out if no column of the
    table sets it (see `_column_sets_field`), so the table isn't filled with
    empty columns; the fields in `_REQUIRED_COLUMN_FIELDS` are always shown so
    every table starts the same.
    """
    return [
        field
        for field in _COLUMN_FIELDS
        if show_empty
        or field in _REQUIRED_COLUMN_FIELDS
        or any(_column_sets_field(column, field) for column in table.columns)
    ]


class ColumnTag(NamedTuple):
    """A tag shown after a column's name in the columns table."""

    kind: str
    """Kind of tag, used in its CSS class: ``primary-key``, ``foreign-key``
    or ``index``.
    """

    label: str
    """Text of the tag."""

    title: str
    """Hover text explaining the tag."""


_TAG_KINDS = ("primary-key", "foreign-key", "index")
"""Kinds of column tag, in the order they are shown."""


def _column_tag(kind: str, details: list[str]) -> ColumnTag:
    if kind == "primary-key":
        return ColumnTag(kind, "primary key", "Part of the table's primary key")
    if kind == "foreign-key":
        return ColumnTag(kind, "foreign key", "References " + ", ".join(details))
    prefix = "In indexes " if len(details) > 1 else "In index "
    return ColumnTag(kind, "indexed", prefix + ", ".join(details))


def _column_tags(schema: Schema, table: Table) -> dict[str, list[ColumnTag]]:
    """Return the tags of a table's columns, by column name, in `_TAG_KINDS`
    order.
    """
    # Each column tagged, with the kind of tag and what it is tagged by.
    found: list[tuple[Column, str, str]] = []
    for column in _primary_key_columns(schema, table):
        found.append((column, "primary-key", ""))
    for constraint in _foreign_keys(table):
        for ref, referenced_ref in zip(constraint.columns, constraint.referenced_columns, strict=True):
            referenced = _resolve_column(schema, referenced_ref)
            target = f"{schema.get_table_by_column(referenced).name}.{referenced.name}"
            found.append((_resolve_column(schema, ref), "foreign-key", target))
    for index in table.indexes:
        for ref in index.columns or []:
            found.append((_resolve_column(schema, ref), "index", index.name))

    # Gather what each column is tagged by, by kind.
    details: dict[str, dict[str, list[str]]] = {}
    for column, kind, detail in found:
        details.setdefault(column.name, {}).setdefault(kind, []).append(detail)

    tags: dict[str, list[ColumnTag]] = {}
    for name, details_by_kind in details.items():
        tags[name] = [
            _column_tag(kind, details_by_kind[kind]) for kind in _TAG_KINDS if kind in details_by_kind
        ]
    return tags
