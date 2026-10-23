"""Lookups in schemas shared by the browser's pages."""

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
from collections.abc import Iterable
from typing import TypeVar

from lsst.utils.iteration import ensure_iterable

from felis.datamodel import BaseObject, Column, ForeignKeyConstraint, Schema, SchemaVersion, Table

_Named = TypeVar("_Named", bound=BaseObject)
"""A schema object of any kind, sorted by name in the sidebar data."""


def _resolve_column(schema: Schema, ref: str) -> Column:
    """Return the column a primary key, index or constraint refers to.

    Raises `ValueError` if ``ref`` isn't a column of the schema. This is the
    only place the browser looks up columns by ID; switch it to the name-based
    lookup from DM-51789 once that is merged, as ID lookups may be removed.
    """
    try:
        return schema.find_object_by_id(ref, Column)
    except (KeyError, TypeError) as e:
        raise ValueError(
            f"Column reference '{ref}' in schema '{schema.name}' is not a column of the schema"
        ) from e


def _primary_key_columns(schema: Schema, table: Table) -> list[Column]:
    """Return the columns of a table's primary key, one reference or a list."""
    return [_resolve_column(schema, ref) for ref in ensure_iterable(table.primary_key or [])]


def _foreign_keys(table: Table) -> list[ForeignKeyConstraint]:
    return [constraint for constraint in table.constraints if isinstance(constraint, ForeignKeyConstraint)]


def _schema_version(schema: Schema) -> str | None:
    if isinstance(schema.version, SchemaVersion):
        return schema.version.current
    return schema.version


def _version_order(version: str | None) -> str:
    """Return a sort key for a version: numbers zero-padded, so 3.8.1 sorts
    before 10.0.0.
    """
    if version is None:
        return ""
    # Splitting on a captured group keeps the numbers as separate parts.
    parts = re.split(r"(\d+)", version)
    return "".join(part.zfill(10) if part.isdigit() else part for part in parts)


def _name_sort_key(obj: BaseObject) -> str:
    return obj.name.lower()


def _sorted_by_name(objects: Iterable[_Named]) -> list[_Named]:
    return sorted(objects, key=_name_sort_key)
