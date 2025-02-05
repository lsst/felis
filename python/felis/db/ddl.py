"""Utilities for DDL generation."""

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

from abc import ABC
from typing import IO

from sqlalchemy import Column, Constraint, Index, MetaData, Table
from sqlalchemy.engine import Dialect
from sqlalchemy.schema import AddConstraint, CreateColumn, CreateIndex, CreateSchema, CreateTable


class DDLGenerator(ABC):
    """Generate DDL statements using a SQLAlchemy dialect."""

    def __init__(
        self,
        dialect: Dialect,
        metadata: MetaData,
        output: IO[str] | None = None,
        single_line_format: bool = False,
    ) -> None:
        if dialect is None:
            raise ValueError("Dialect is required")
        self.dialect = dialect
        self.metadata = metadata
        self.single_line_format = single_line_format
        self.output = output

    def _get_table(self, table_name: str) -> Table:
        if self.metadata.schema is not None:
            table_name = f"{self.metadata.schema}.{table_name}"
        table = self.metadata.tables.get(table_name)
        if table is None:
            raise ValueError(f"Table '{table_name}' not found in metadata")
        return table

    def _get_column(self, table_name: str, column_name: str) -> Column:
        column = self._get_table(table_name).columns.get(column_name)
        if column is None:
            raise ValueError(f"Column '{column_name}' not found in table '{table_name}'")
        return column

    def _get_index(self, table_name: str, index_name: str) -> Index:
        table = self._get_table(table_name)
        for index in table.indexes:
            if index.name == index_name:
                return index
        raise ValueError(f"Index '{index_name}' not found in table '{table_name}'")

    def _get_constraint(self, table_name: str, constraint_name: str) -> Constraint:
        table = self._get_table(table_name)
        for constraint in table.constraints:
            if constraint.name == constraint_name:
                return constraint
        raise ValueError(f"Constraint '{constraint_name}' not found in table '{table_name}'")

    def _get_schema_name(self) -> str:
        return self.metadata.schema if self.metadata.schema is not None else ""

    def create_table(self, table_name: str) -> str:
        """Generate the SQL for creating a table.

        Parameters
        ----------
        table_name
            The name of the table to create.
        """
        table = self._get_table(table_name)
        sql = str(CreateTable(table).compile(dialect=self.dialect))
        if self.single_line_format:
            sql = " ".join(sql.split())
        sql += ";"
        if self.output is not None:
            print(sql, file=self.output)
        return sql

    def create_index(self, table_name: str, index_name: str) -> str:
        """Generate the SQL for creating an index.

        Parameters
        ----------
        table_name
            The name of the table containing the index.
        """
        index = self._get_index(table_name, index_name)
        if index is None:
            raise ValueError(f"Index '{index_name}' not found in table '{table_name}'")
        sql = str(CreateIndex(index).compile(dialect=self.dialect))
        if self.single_line_format:
            sql = " ".join(sql.split())
        sql += ";"
        if self.output is not None:
            print(sql, file=self.output)
        return sql

    def add_constraint(self, table_name: str, constraint_name: str) -> str:
        """Generate the SQL for creating a constraint.

        Parameters
        ----------
        table_name
            The name of the table containing the constraint.
        """
        constraint = self._get_constraint(table_name, constraint_name)
        if constraint is None:
            raise ValueError(f"Constraint '{constraint_name}' not found in table '{table_name}'")
        sql = str(AddConstraint(constraint).compile(dialect=self.dialect))
        if self.single_line_format:
            sql = " ".join(sql.split())
        sql += ";"
        if self.output is not None:
            print(sql, file=self.output)
        return sql

    def create_schema(self) -> str:
        """Generate the SQL for creating a schema.

        Parameters
        ----------
        schema_name
            The name of the schema to create.
        """
        schema_name = self.metadata.schema
        if schema_name is None:
            raise ValueError("Schema name is required")
        sql = str(CreateSchema(schema_name, if_not_exists=True).compile(dialect=self.dialect)) + ";"
        if self.output is not None:
            print(sql, file=self.output)
        return sql

    def create_column(self, table_name: str, column_name: str) -> str:
        """Generate the SQL for creating a column.

        Parameters
        ----------
        column_name
            The name of the column to create.
        """
        col = self._get_column(table_name, column_name)
        sql = str(CreateColumn(col).compile(dialect=self.dialect))
        if self.output is not None:
            print(sql, file=self.output)
        return sql
