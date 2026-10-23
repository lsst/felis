"""Where each schema, table, column, index and constraint is in the site: page
paths and element IDs.
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

import logging
import os
import re
from collections.abc import Iterable
from pathlib import Path

from felis.datamodel import BaseObject, Schema, Table

from .schemas import _sorted_by_name

logger = logging.getLogger(__name__)


def _slugify(value: str) -> str:
    """Return a URL-safe slug of lowercase letters, digits and hyphens
    ("schema" if empty).
    """
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "schema"


def _relative_href(current_output_path: str, target_output_path: str) -> str:
    """Build a POSIX relative href from one output HTML path to another."""
    current_dir = Path(current_output_path).parent
    return Path(os.path.relpath(target_output_path, start=current_dir)).as_posix()


class _UniqueNames:
    """Hand out unique names, appending ``-2``, ``-3``, ... to repeats."""

    def __init__(self) -> None:
        self._issued: set[str] = set()
        self._counts: dict[str, int] = {}

    def claim(self, base: str) -> str:
        # base, or for a repeat base-N with the lowest N from 2 not already
        # handed out, so a repeat never takes a name another base got as
        # itself (such as "foo-2").
        name = base
        count = self._counts.get(base, 1)
        while name in self._issued:
            count += 1
            name = f"{base}-{count}"
        self._counts[base] = count
        self._issued.add(name)
        return name


class SiteMap:
    """Output paths and element IDs of all objects shown in the site.

    Objects are keyed by ``id()`` because the felis models are not hashable;
    the schemas must outlive the site map.

    Parameters
    ----------
    schemas
        The schemas in the site. Paths are assigned in this order, so the
        order decides which of two same-named schemas gets a ``-2`` suffix.
    """

    def __init__(self, schemas: Iterable[Schema]) -> None:
        self._paths: dict[int, str] = {}
        self._anchors: dict[int, str] = {}
        self._tables: dict[int, Table] = {}

        schema_dirs = _UniqueNames()
        schemas = list(schemas)
        for schema in schemas:
            logger.debug("Mapping schema: %s", schema.name)
            schema_dir = f"schemas/{schema_dirs.claim(_slugify(schema.name))}"
            self._paths[id(schema)] = f"{schema_dir}/index.html"
            table_files = _UniqueNames()
            for table in schema.tables:
                table_file = _slugify(table.name)
                # The schema page is the directory's index.html.
                if table_file == "index":
                    raise ValueError(f"Invalid table name '{table.name}' in schema '{schema.name}'")
                self._paths[id(table)] = f"{schema_dir}/{table_files.claim(table_file)}.html"
                self._anchors[id(table)] = f"table-{_slugify(table.name)}"
                self._add_rows(table, "column", table.columns)
                self._add_rows(table, "index", table.indexes)
                self._add_rows(table, "constraint", table.constraints)

        self.schemas = _sorted_by_name(schemas)
        """The schemas in display order."""

    def _add_rows(self, table: Table, prefix: str, objects: Iterable[BaseObject]) -> None:
        """Record the table and a unique element ID of each row object on a
        table's page.
        """
        names = _UniqueNames()
        for obj in objects:
            self._anchors[id(obj)] = names.claim(f"{prefix}-{_slugify(obj.name)}")
            self._tables[id(obj)] = table

    def path(self, obj: BaseObject) -> str:
        """Return the location of ``obj`` within the site.

        Parameters
        ----------
        obj
            A schema or an object in one.

        Returns
        -------
        str
            The page of a schema or table, otherwise the object's anchor on
            its table's page, relative to the site root.
        """
        if id(obj) in self._paths:
            return self._paths[id(obj)]
        return f"{self._paths[id(self.table_of(obj))]}#{self.anchor(obj)}"

    def anchor(self, obj: BaseObject) -> str:
        """Return the element ID of ``obj`` on its table's page.

        Parameters
        ----------
        obj
            A schema or an object in one.

        Returns
        -------
        str
            The element ID.
        """
        return self._anchors[id(obj)]

    def table_of(self, obj: BaseObject) -> Table:
        """Return the table containing ``obj``.

        Unlike `Schema.get_table_by_column`, this covers indexes and
        constraints as well as columns, and is a lookup rather than a search
        of the schema's tables.

        Parameters
        ----------
        obj
            A schema or an object in one.

        Returns
        -------
        `Table`
            The table whose page shows ``obj``.
        """
        return self._tables[id(obj)]
