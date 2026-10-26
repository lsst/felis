"""The relationship diagram of a schema's foreign keys, drawn by Mermaid in
the browser.
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

import re

from felis.datamodel import ForeignKeyConstraint, Schema, Table

from .schemas import _foreign_keys, _primary_key_columns, _resolve_column


def _mermaid_label(text: str) -> str:
    """Return text for a quoted Mermaid label: " becomes ', and % and
    backslashes are removed.
    """
    return re.sub(r"[%\\]", "", text.replace('"', "'"))


def _mermaid_name(text: str) -> str:
    """Return a Mermaid attribute name: letters, digits, _ and -, not starting
    with a digit.
    """
    name = re.sub(r"[^A-Za-z0-9_-]", "_", text)
    if not re.match(r"[A-Za-z_]", name):
        name = f"_{name}"
    return name


class _RelationshipDiagram:
    """The Mermaid entity-relationship diagram of a schema's foreign keys.

    It shows the tables involved in foreign keys, in schema order, with their
    primary and foreign key columns, and one relationship per foreign key,
    labelled with its name. Tables are keyed t0, t1, ... in the diagram text.
    """

    def __init__(self, schema: Schema) -> None:
        self._schema = schema
        # Each foreign key, with the table it is defined on.
        self._foreign_keys = [
            (table, constraint) for table in schema.tables for constraint in _foreign_keys(table)
        ]

    def text(self) -> str | None:
        """Return the diagram's Mermaid text, or `None` if there are no foreign
        keys.
        """
        if not self._foreign_keys:
            return None
        tables = self._tables()
        keys = {table.name: f"t{number}" for number, table in enumerate(tables)}
        lines = ["erDiagram"]
        for table in tables:
            lines += self._entity_lines(table, keys[table.name])
        for table, constraint in self._foreign_keys:
            parent = self._referenced_table(constraint)
            label = _mermaid_label(constraint.name)
            lines.append(f'  {keys[table.name]} }}o--|| {keys[parent.name]} : "{label}"')
        return "\n".join(lines)

    def _referenced_table(self, constraint: ForeignKeyConstraint) -> Table:
        referenced = _resolve_column(self._schema, constraint.referenced_columns[0])
        return self._schema.get_table_by_column(referenced)

    def _tables(self) -> list[Table]:
        names = set()
        for table, constraint in self._foreign_keys:
            names.add(table.name)
            names.add(self._referenced_table(constraint).name)
        return [table for table in self._schema.tables if table.name in names]

    def _entity_lines(self, table: Table, key: str) -> list[str]:
        primary_key = {column.name for column in _primary_key_columns(self._schema, table)}
        foreign_key = set()
        for constraint in _foreign_keys(table):
            for ref in constraint.columns:
                foreign_key.add(_resolve_column(self._schema, ref).name)
        lines = [f'  {key}["{_mermaid_label(table.name)}"] {{']
        for column in table.columns:
            marks = []
            if column.name in primary_key:
                marks.append("PK")
            if column.name in foreign_key:
                marks.append("FK")
            if marks:
                # Mermaid draws its "type name" pairs in that order; writing
                # the name first shows it in the first column.
                lines.append(f"    {_mermaid_name(column.name)} {column.datatype} {', '.join(marks)}")
        lines.append("  }")
        return lines
