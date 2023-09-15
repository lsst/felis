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

__all__ = ["SQLVisitor"]

import logging
import re
from collections.abc import Iterable, Mapping, MutableMapping
from typing import Any, NamedTuple

from sqlalchemy import (
    CheckConstraint,
    Column,
    Constraint,
    ForeignKeyConstraint,
    Index,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    UniqueConstraint,
    types,
)
from sqlalchemy.dialects import mysql, oracle, postgresql, sqlite
from sqlalchemy.schema import Table

from .check import FelisValidator
from .db import sqltypes
from .types import FelisType
from .visitor import Visitor

_Mapping = Mapping[str, Any]
_MutableMapping = MutableMapping[str, Any]

logger = logging.getLogger("felis")

MYSQL = "mysql"
ORACLE = "oracle"
POSTGRES = "postgresql"
SQLITE = "sqlite"

TABLE_OPTS = {
    "mysql:engine": "mysql_engine",
    "mysql:charset": "mysql_charset",
    "oracle:compress": "oracle_compress",
}

COLUMN_VARIANT_OVERRIDE = {
    "mysql:datatype": "mysql",
    "oracle:datatype": "oracle",
    "postgresql:datatype": "postgresql",
    "sqlite:datatype": "sqlite",
}

DIALECT_MODULES = {MYSQL: mysql, ORACLE: oracle, SQLITE: sqlite, POSTGRES: postgresql}

length_regex = re.compile(r"\((.+)\)")


class Schema(NamedTuple):
    name: str | None
    tables: list[Table]
    metadata: MetaData
    graph_index: Mapping[str, Any]


class SQLVisitor(Visitor[Schema, Table, Column, PrimaryKeyConstraint | None, Constraint, Index, None]):
    """A Felis Visitor which populates a SQLAlchemy metadata object.

    Parameters
    ----------
    schema_name : `str`, optional
        Override for the schema name.
    """

    def __init__(self, schema_name: str | None = None):
        self.metadata = MetaData()
        self.schema_name = schema_name
        self.checker = FelisValidator()
        self.graph_index: MutableMapping[str, Any] = {}

    def visit_schema(self, schema_obj: _Mapping) -> Schema:
        # Docstring is inherited.
        self.checker.check_schema(schema_obj)
        if (version_obj := schema_obj.get("version")) is not None:
            self.visit_schema_version(version_obj, schema_obj)
        schema = Schema(
            name=self.schema_name or schema_obj["name"],
            tables=[self.visit_table(t, schema_obj) for t in schema_obj["tables"]],
            metadata=self.metadata,
            graph_index=self.graph_index,
        )
        return schema

    def visit_schema_version(
        self, version_obj: str | Mapping[str, Any], schema_obj: Mapping[str, Any]
    ) -> None:
        # Docstring is inherited.

        # For now we ignore schema versioning completely, still do some checks.
        self.checker.check_schema_version(version_obj, schema_obj)

    def visit_table(self, table_obj: _Mapping, schema_obj: _Mapping) -> Table:
        # Docstring is inherited.
        self.checker.check_table(table_obj, schema_obj)
        columns = [self.visit_column(c, table_obj) for c in table_obj["columns"]]

        name = table_obj["name"]
        table_id = table_obj["@id"]
        description = table_obj.get("description")
        schema_name = self.schema_name or schema_obj["name"]

        table = Table(name, self.metadata, *columns, schema=schema_name, comment=description)

        primary_key = self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)
        if primary_key:
            table.append_constraint(primary_key)

        constraints = [self.visit_constraint(c, table_obj) for c in table_obj.get("constraints", [])]
        for constraint in constraints:
            table.append_constraint(constraint)

        indexes = [self.visit_index(i, table_obj) for i in table_obj.get("indexes", [])]
        for index in indexes:
            # FIXME: Hack because there's no table.add_index
            index._set_parent(table)
            table.indexes.add(index)
        self.graph_index[table_id] = table
        return table

    def visit_column(self, column_obj: _Mapping, table_obj: _Mapping) -> Column:
        # Docstring is inherited.
        self.checker.check_column(column_obj, table_obj)
        column_name = column_obj["name"]
        column_id = column_obj["@id"]
        datatype_name = column_obj["datatype"]
        column_description = column_obj.get("description")
        column_default = column_obj.get("value")
        column_length = column_obj.get("length")

        kwargs = {}
        for column_opt in column_obj.keys():
            if column_opt in COLUMN_VARIANT_OVERRIDE:
                dialect = COLUMN_VARIANT_OVERRIDE[column_opt]
                variant = _process_variant_override(dialect, column_obj[column_opt])
                kwargs[dialect] = variant

        felis_type = FelisType.felis_type(datatype_name)
        datatype_fun = getattr(sqltypes, datatype_name)

        if felis_type.is_sized:
            datatype = datatype_fun(column_length, **kwargs)
        else:
            datatype = datatype_fun(**kwargs)

        nullable_default = True
        if isinstance(datatype, Numeric):
            nullable_default = False

        column_nullable = column_obj.get("nullable", nullable_default)
        column_autoincrement = column_obj.get("autoincrement", "auto")

        column: Column = Column(
            column_name,
            datatype,
            comment=column_description,
            autoincrement=column_autoincrement,
            nullable=column_nullable,
            server_default=column_default,
        )
        if column_id in self.graph_index:
            logger.warning(f"Duplication of @id {column_id}")
        self.graph_index[column_id] = column
        return column

    def visit_primary_key(
        self, primary_key_obj: str | Iterable[str], table_obj: _Mapping
    ) -> PrimaryKeyConstraint | None:
        # Docstring is inherited.
        self.checker.check_primary_key(primary_key_obj, table_obj)
        if primary_key_obj:
            if isinstance(primary_key_obj, str):
                primary_key_obj = [primary_key_obj]
            columns = [self.graph_index[c_id] for c_id in primary_key_obj]
            return PrimaryKeyConstraint(*columns)
        return None

    def visit_constraint(self, constraint_obj: _Mapping, table_obj: _Mapping) -> Constraint:
        # Docstring is inherited.
        self.checker.check_constraint(constraint_obj, table_obj)
        constraint_type = constraint_obj["@type"]
        constraint_id = constraint_obj["@id"]

        constraint_args: _MutableMapping = {}
        # The following are not used on every constraint
        _set_if("name", constraint_obj.get("name"), constraint_args)
        _set_if("info", constraint_obj.get("description"), constraint_args)
        _set_if("expression", constraint_obj.get("expression"), constraint_args)
        _set_if("deferrable", constraint_obj.get("deferrable"), constraint_args)
        _set_if("initially", constraint_obj.get("initially"), constraint_args)

        columns = [self.graph_index[c_id] for c_id in constraint_obj.get("columns", [])]
        constraint: Constraint
        if constraint_type == "ForeignKey":
            refcolumns = [self.graph_index[c_id] for c_id in constraint_obj.get("referencedColumns", [])]
            constraint = ForeignKeyConstraint(columns, refcolumns, **constraint_args)
        elif constraint_type == "Check":
            expression = constraint_obj["expression"]
            constraint = CheckConstraint(expression, **constraint_args)
        elif constraint_type == "Unique":
            constraint = UniqueConstraint(*columns, **constraint_args)
        else:
            raise ValueError(f"Unexpected constraint type: {constraint_type}")
        self.graph_index[constraint_id] = constraint
        return constraint

    def visit_index(self, index_obj: _Mapping, table_obj: _Mapping) -> Index:
        # Docstring is inherited.
        self.checker.check_index(index_obj, table_obj)
        name = index_obj["name"]
        description = index_obj.get("description")
        columns = [self.graph_index[c_id] for c_id in index_obj.get("columns", [])]
        expressions = index_obj.get("expressions", [])
        return Index(name, *columns, *expressions, info=description)


def _set_if(key: str, value: Any, mapping: _MutableMapping) -> None:
    if value is not None:
        mapping[key] = value


def _process_variant_override(dialect_name: str, variant_override_str: str) -> types.TypeEngine:
    """Return variant type for given dialect."""
    match = length_regex.search(variant_override_str)
    dialect = DIALECT_MODULES[dialect_name]
    variant_type_name = variant_override_str.split("(")[0]

    # Process Variant Type
    if variant_type_name not in dir(dialect):
        raise ValueError(f"Type {variant_type_name} not found in dialect {dialect_name}")
    variant_type = getattr(dialect, variant_type_name)
    length_params = []
    if match:
        length_params.extend([int(i) for i in match.group(1).split(",")])
    return variant_type(*length_params)
