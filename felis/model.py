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
# along with this program. If not, see <https://www.gnu.org/licenses/>.

import logging
import re

from sqlalchemy import MetaData, Column, Numeric, ForeignKeyConstraint, \
    CheckConstraint, UniqueConstraint, PrimaryKeyConstraint, Index
from sqlalchemy.dialects import mysql, oracle, postgresql, sqlite
from sqlalchemy.schema import Table

from .db import sqltypes
from .felistypes import TYPE_NAMES, LENGTH_TYPES, DATETIME_TYPES

logger = logging.getLogger("felis")

MYSQL = "mysql"
ORACLE = "oracle"
POSTGRES = "postgresql"
SQLITE = "sqlite"

TABLE_OPTS = {
    "mysql:engine": "mysql_engine",
    "mysql:charset": "mysql_charset",
    "oracle:compress": "oracle_compress"
}

COLUMN_VARIANT_OVERRIDE = {
    "mysql:datatype": "mysql",
    "oracle:datatype": "oracle",
    "postgresql:datatype": "postgresql",
    "sqlite:datatype": "sqlite"
}

DIALECT_MODULES = {
    MYSQL: mysql,
    ORACLE: oracle,
    SQLITE: sqlite,
    POSTGRES: postgresql
}

length_regex = re.compile(r'\((.+)\)')


class Schema:
    pass


class VisitorBase:
    """
    Base class for visitors. Includes the graph_index and functions for
    validating objects.
    """
    def __init__(self):
        super().__init__()
        self.graph_index = {}

    def assert_id(self, obj):
        _id = obj.get("@id")
        if not _id:
            name = obj.get("name", "")
            maybe_string = f"(check object with name: {name})" if name else ""
            raise ValueError(f"No @id defined for object {maybe_string}")

    def assert_name(self, obj):
        _id = obj.get("@id")
        if "name" not in obj:
            raise ValueError(f"No name for table object {_id}")

    def assert_datatype(self, obj):
        datatype_name = obj.get("datatype")
        _id = obj["@id"]
        if not datatype_name:
            raise ValueError(f"No datatype defined for id {_id}")
        if datatype_name not in TYPE_NAMES:
            raise ValueError(f"Incorrect Type Name for id {_id}: {datatype_name}")

    def check_visited(self, _id):
        if _id in self.graph_index:
            logger.warning(f"Duplication of @id {_id}")

    def check_table(self, table_obj, schema_obj):
        self.assert_id(table_obj)
        self.assert_name(table_obj)
        _id = table_obj["@id"]
        self.check_visited(_id)

    def check_column(self, column_obj, table_obj):
        self.assert_id(column_obj)
        self.assert_name(column_obj)
        _id = column_obj["@id"]
        self.assert_datatype(column_obj)
        datatype_name = column_obj.get("datatype")
        length = column_obj.get("length")
        if not length and (datatype_name in LENGTH_TYPES or datatype_name in DATETIME_TYPES):
            # This is not a warning, because it's usually fine
            logger.info(f"No length defined for {_id} for type {datatype_name}")
        self.check_visited(_id)

    def check_primary_key(self, primary_key_obj, table):
        pass

    def check_constraint(self, constraint_obj, table_obj):
        self.assert_id(constraint_obj)
        _id = constraint_obj["@id"]
        constraint_type = constraint_obj.get("@type")
        if not constraint_type:
            raise ValueError(f"Constraint has no @type: {_id}")
        if constraint_type not in ["ForeignKey", "Check", "Unique"]:
            raise ValueError(f"Not a valid constraint type: {constraint_type}")
        self.check_visited(_id)

    def check_index(self, index_obj, table_obj):
        self.assert_id(index_obj)
        _id = index_obj["@id"]
        self.assert_name(index_obj)
        if "columns" in index_obj and "expressions" in index_obj:
            raise ValueError(f"Defining columns and expressions is not valid for index {_id}")
        self.check_visited(_id)

    def visit_schema(self, schema_obj):
        self.assert_id(schema_obj)
        self.graph_index[schema_obj["@id"]] = schema_obj
        for table_obj in schema_obj["tables"]:
            self.visit_table(table_obj, schema_obj)

    def visit_table(self, table_obj, schema_obj):
        self.check_table(table_obj, schema_obj)
        self.graph_index[table_obj["@id"]] = table_obj
        for column_obj in table_obj["columns"]:
            self.visit_column(column_obj, table_obj)
        self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)
        for constraint_obj in table_obj.get("constraints", []):
            self.visit_constraint(constraint_obj, table_obj)
        for index_obj in table_obj.get("indexes", []):
            self.visit_index(index_obj, table_obj)

    def visit_column(self, column_obj, table_obj):
        self.check_column(column_obj, table_obj)
        self.graph_index[column_obj["@id"]] = column_obj

    def visit_primary_key(self, primary_key_obj, table_obj):
        self.check_primary_key(primary_key_obj, table_obj)

    def visit_constraint(self, constraint_obj, table_obj):
        self.check_constraint(constraint_obj, table_obj)
        self.graph_index[constraint_obj["@id"]] = constraint_obj

    def visit_index(self, index_obj, table_obj):
        self.check_index(index_obj, table_obj)
        self.graph_index[index_obj["@id"]] = index_obj

class Visitor(VisitorBase):
    def __init__(self, schema_name=None):
        """
        A Visitor which populates a SQLAlchemy metadata object.
        :param schema_name: Override the schema name
        """
        super(Visitor, self).__init__()
        self.metadata = MetaData()
        self.schema_name = schema_name

    def visit_schema(self, schema_obj):
        schema = Schema()
        schema.name = self.schema_name or schema_obj["name"]
        schema.tables = [self.visit_table(t, schema_obj) for t in schema_obj["tables"]]
        schema.metadata = self.metadata
        schema.graph_index = self.graph_index
        return schema

    def visit_table(self, table_obj, schema_obj):
        self.check_table(table_obj, schema_obj)
        columns = [self.visit_column(c, table_obj) for c in table_obj["columns"]]

        name = table_obj["name"]
        table_id = table_obj["@id"]
        description = table_obj.get("description")
        schema_name = self.schema_name or schema_obj["name"]

        table = Table(
            name,
            self.metadata,
            *columns,
            schema=schema_name,
            comment=description
        )

        primary_key = self.visit_primary_key(table_obj.get("primaryKey", []), table_obj)
        if primary_key:
            table.append_constraint(primary_key)

        primary_key = self.visit_primary_key(table_obj.get("primaryKey"), table)
        if primary_key:
            table.append_constraint(primary_key)

        constraints = [self.visit_constraint(c, table) for c in table_obj.get("constraints", [])]
        for constraint in constraints:
            table.append_constraint(constraint)

        indexes = [self.visit_index(i, table) for i in table_obj.get("indexes", [])]
        for index in indexes:
            # FIXME: Hack because there's no table.add_index
            index._set_parent(table)
            table.indexes.add(index)
        self.graph_index[table_id] = table

    def visit_column(self, column_obj, table_obj):
        self.check_column(column_obj, table_obj)
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

        datatype_fun = getattr(sqltypes, datatype_name)

        if datatype_fun.__name__ in LENGTH_TYPES:
            datatype = datatype_fun(column_length, **kwargs)
        else:
            datatype = datatype_fun(**kwargs)

        nullable_default = True
        if isinstance(datatype, Numeric):
            nullable_default = False

        column_nullable = column_obj.get("nullable", nullable_default)
        column_autoincrement = column_obj.get("autoincrement", "auto")

        column = Column(
            column_name,
            datatype,
            comment=column_description,
            autoincrement=column_autoincrement,
            nullable=column_nullable,
            server_default=column_default
        )
        if column_id in self.graph_index:
            logger.warning(f"Duplication of @id {column_id}")
        self.graph_index[column_id] = column
        return column

    def visit_primary_key(self, primary_key_obj, table_obj):
        self.check_primary_key(primary_key_obj, table_obj)
        if primary_key_obj:
            if not isinstance(primary_key_obj, list):
                primary_key_obj = [primary_key_obj]
            columns = [
                self.graph_index[c_id] for c_id in primary_key_obj
            ]
            return PrimaryKeyConstraint(*columns)
        return None

    def visit_constraint(self, constraint_obj, table_obj):
        self.check_constraint(constraint_obj, table_obj)
        constraint_type = constraint_obj["@type"]
        constraint_id = constraint_obj["@id"]

        constraint_args = {}
        # The following are not used on every constraint
        _set_if("name", constraint_obj.get("name"), constraint_args)
        _set_if("info", constraint_obj.get("description"), constraint_args)
        _set_if("expression", constraint_obj.get("expression"), constraint_args)
        _set_if("deferrable", constraint_obj.get("deferrable"), constraint_args)
        _set_if("initially", constraint_obj.get("initially"), constraint_args)

        columns = [
            self.graph_index[c_id] for c_id in constraint_obj.get("columns", [])
        ]
        if constraint_type == "ForeignKey":
            refcolumns = [
                self.graph_index[c_id] for c_id in constraint_obj.get("referencedColumns", [])
            ]
            constraint = ForeignKeyConstraint(columns, refcolumns, **constraint_args)
        elif constraint_type == "Check":
            expression = constraint_obj["expression"]
            constraint = CheckConstraint(expression, **constraint_args)
        elif constraint_type == "Unique":
            constraint = UniqueConstraint(*columns, **constraint_args)
        self.graph_index[constraint_id] = constraint
        return constraint

    def visit_index(self, index_obj, table_obj):
        self.check_index(index_obj, table_obj)
        name = index_obj["name"]
        description = index_obj.get("description")
        columns = [
            self.graph_index[c_id] for c_id in index_obj.get("columns", [])
        ]
        expressions = index_obj.get("expressions", [])
        return Index(name, *columns, *expressions, info=description)


def _set_if(key, value, mapping):
    if value is not None:
        mapping[key] = value


def _process_variant_override(dialect_name, variant_override_str):
    """Simple Data Type Override"""
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
