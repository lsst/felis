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
from collections.abc import Sequence
from typing import Any

from pydantic import Field, model_validator

from .datamodel import Column, DescriptionStr, Schema, Table

logger = logging.getLogger(__name__)

__all__ = ["RspColumn", "RspSchema", "RspTable", "get_schema"]


class RspColumn(Column):
    """Column for RSP data validation."""

    description: DescriptionStr
    """Redefine description to make it required."""


class RspTable(Table):
    """Table for RSP data validation.

    The list of columns is overridden to use RspColumn instead of Column.

    Tables for the RSP must have a TAP table index and a valid description.
    """

    description: DescriptionStr
    """Redefine description to make it required."""

    tap_table_index: int = Field(..., alias="tap:table_index")
    """Redefine the TAP_SCHEMA table index so that it is required."""

    columns: Sequence[RspColumn]
    """Redefine columns to include RSP validation."""

    @model_validator(mode="after")  # type: ignore[arg-type]
    @classmethod
    def check_tap_principal(cls: Any, tbl: "RspTable") -> "RspTable":
        """Check that at least one column is flagged as 'principal' for
        TAP purposes.
        """
        for col in tbl.columns:
            if col.tap_principal == 1:
                return tbl
        raise ValueError(f"Table '{tbl.name}' is missing at least one column designated as 'tap:principal'")


class RspSchema(Schema):
    """Schema for RSP data validation.

    TAP table indexes must be unique across all tables.
    """

    tables: Sequence[RspTable]
    """Redefine tables to include RSP validation."""

    @model_validator(mode="after")  # type: ignore[arg-type]
    @classmethod
    def check_tap_table_indexes(cls: Any, sch: RspSchema) -> RspSchema:
        """Check that the TAP table indexes are unique."""
        table_indicies = set()
        for table in sch.tables:
            table_index = table.tap_table_index
            if table_index is not None:
                if table_index in table_indicies:
                    raise ValueError(f"Duplicate 'tap:table_index' value {table_index} found in schema")
                table_indicies.add(table_index)
        return sch


def get_schema(schema_name: str) -> type[Schema]:
    """Get the schema class for the given name."""
    if schema_name == "default":
        return Schema
    elif schema_name == "RSP":
        return RspSchema
    else:
        raise ValueError(f"Unknown schema name '{schema_name}'")
