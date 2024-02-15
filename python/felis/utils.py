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

from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any

_Mapping = Mapping[str, Any]
_MutableMapping = MutableMapping[str, Any]


class ReorderingVisitor:
    """A visitor that reorders and optionally adds the "@type".

    Parameters
    ----------
    add_type : `bool`
        If true, add the "@type" if it doesn't exist.
    """

    def __init__(self, add_type: bool = False):
        self.add_type = add_type

    def visit_schema(self, schema_obj: _MutableMapping) -> _Mapping:
        """Process schema, the input MUST be a normalized representation."""
        # Override with default
        tables = [self.visit_table(table_obj, schema_obj) for table_obj in schema_obj["tables"]]
        schema_obj["tables"] = tables
        if self.add_type:
            schema_obj["@type"] = schema_obj.get("@type", "Schema")
        return _new_order(
            schema_obj, ["@context", "name", "@id", "@type", "description", "tables", "version"]
        )

    def visit_table(self, table_obj: _MutableMapping, schema_obj: _Mapping) -> _Mapping:
        columns = [self.visit_column(c, table_obj) for c in table_obj["columns"]]
        primary_key = self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)
        constraints = [self.visit_constraint(c, table_obj) for c in table_obj.get("constraints", [])]
        indexes = [self.visit_index(i, table_obj) for i in table_obj.get("indexes", [])]
        table_obj["columns"] = columns
        if primary_key:
            table_obj["primaryKey"] = primary_key
        if constraints:
            table_obj["constraints"] = constraints
        if indexes:
            table_obj["indexes"] = indexes
        if self.add_type:
            table_obj["@type"] = table_obj.get("@type", "Table")
        return _new_order(
            table_obj,
            ["name", "@id", "@type", "description", "columns", "primaryKey", "constraints", "indexes"],
        )

    def visit_column(self, column_obj: _MutableMapping, table_obj: _Mapping) -> _Mapping:
        if self.add_type:
            column_obj["@type"] = column_obj.get("@type", "Column")
        return _new_order(column_obj, ["name", "@id", "@type", "description", "datatype"])

    def visit_primary_key(self, primary_key_obj: _MutableMapping, table: _Mapping) -> _Mapping:
        # FIXME: Handle Primary Keys
        return primary_key_obj

    def visit_constraint(self, constraint_obj: _MutableMapping, table: _Mapping) -> _Mapping:
        # Type MUST be present... we can skip
        return _new_order(constraint_obj, ["name", "@id", "@type", "description"])

    def visit_index(self, index_obj: _MutableMapping, table: _Mapping) -> _Mapping:
        if self.add_type:
            index_obj["@type"] = index_obj.get("@type", "Index")
        return _new_order(index_obj, ["name", "@id", "@type", "description"])


def _new_order(obj: _Mapping, order: Iterable[str]) -> _Mapping:
    reordered_object: _MutableMapping = {}
    for name in order:
        if name in obj:
            reordered_object[name] = obj[name]
    for key, value in obj.items():
        if key not in reordered_object:
            reordered_object[key] = value
    return reordered_object
