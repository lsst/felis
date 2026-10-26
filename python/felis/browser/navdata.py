"""The navigation data shared by all pages, for the sidebar tree and the
search.
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

from typing import Any

from felis.datamodel import BaseObject, Column, Schema, Table

from .schemas import _sorted_by_name
from .sitemap import SiteMap


class _NavData:
    """The navigation data shared by all pages, which the sidebar tree and the
    search are built from.

    It lists the schemas, their tables, and each table's columns (with UCDs),
    indexes and constraints, sorted by name. Paths are relative to the site
    root; empty descriptions and UCDs are left out.
    """

    def __init__(self, site: SiteMap) -> None:
        self._site = site

    def data(self) -> dict[str, Any]:
        return {"schemas": [self._schema(schema) for schema in self._site.schemas]}

    def _schema(self, schema: Schema) -> dict[str, Any]:
        tables = [self._table(table) for table in schema.tables]
        return self._entry(schema, path=self._site.path(schema), tables=tables)

    def _table(self, table: Table) -> dict[str, Any]:
        return self._entry(
            table,
            path=self._site.path(table),
            anchor=self._site.anchor(table),
            columns=[self._column(column) for column in _sorted_by_name(table.columns)],
            indexes=[self._anchored(index) for index in _sorted_by_name(table.indexes)],
            constraints=[self._anchored(constraint) for constraint in _sorted_by_name(table.constraints)],
        )

    def _column(self, column: Column) -> dict[str, Any]:
        entry = self._anchored(column)
        if column.ivoa_ucd:
            entry["ucd"] = column.ivoa_ucd
        return entry

    def _anchored(self, obj: BaseObject) -> dict[str, Any]:
        return self._entry(obj, anchor=self._site.anchor(obj))

    @staticmethod
    def _entry(obj: BaseObject, **fields: Any) -> dict[str, Any]:
        """Return an object's entry: its name, the fields, and its description
        if any.
        """
        entry = {"name": obj.name, **fields}
        if obj.description:
            entry["description"] = obj.description
        return entry
