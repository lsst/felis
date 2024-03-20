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
from io import TextIOBase
from typing import Any, Literal

import sqlalchemy.schema as sqa_schema
from sqlalchemy import (
    CheckConstraint,
    Column,
    Constraint,
    Engine,
    ForeignKeyConstraint,
    Index,
    MetaData,
    Numeric,
    PrimaryKeyConstraint,
    ResultProxy,
    Table,
    UniqueConstraint,
    create_mock_engine,
    make_url,
    text,
)
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError

from felis.datamodel import Schema
from felis.db._variants import make_variant_dict

from . import datamodel as dm
from .db import sqltypes
from .types import FelisType

logger = logging.getLogger(__name__)


class InsertDump:
    """An Insert Dumper for SQL statements which supports writing messages
    to stdout or a file.

    Copied and modified slightly from `cli.py` in Felis as that class may be
    removed soon.
    """

    def __init__(self, file: TextIOBase | None = None) -> None:
        """Initialize the insert dumper.

        Parameters
        ----------
        file : `TextIOBase` or None, optional
            The file to write the SQL statements to. If None, the statements
            will be written to stdout.
        """
        self.file = file
        self.dialect: Any | None = None

    def dump(self, sql: Any, *multiparams: Any, **params: Any) -> None:
        """Dump the SQL statement to a file or stdout.

        Parameters
        ----------
        sql : `Any`
            The SQL statement to dump.

        multiparams : `Any`
            The multiparams to use for the SQL statement.
        params : `Any`
            The params to use for the SQL statement.
        """
        compiled = sql.compile(dialect=self.dialect)
        sql_str = str(compiled) + ";"
        params_list = [compiled.params]
        for params in params_list:
            if not params:
                print(sql_str, file=self.file)
                continue
            new_params = {}
            for key, value in params.items():
                if isinstance(value, str):
                    new_params[key] = f"'{value}'"
                elif value is None:
                    new_params[key] = "null"
                else:
                    new_params[key] = value
            print(sql_str % new_params, file=self.file)


class MetaDataBuilder:
    """A class for building a `MetaData` object from a `Schema` object."""

    def __init__(
        self, schema: Schema, apply_schema_to_metadata: bool = True, apply_schema_to_tables: bool = True
    ) -> None:
        self.schema = schema
        if not apply_schema_to_metadata:
            logger.debug("Schema name will not be applied to metadata")
        if not apply_schema_to_tables:
            logger.debug("Schema name will not be applied to tables")
        self.metadata = MetaData(schema=schema.name if apply_schema_to_metadata else None)
        self._objects: dict[str, Any] = {}
        self.apply_schema_to_tables = apply_schema_to_tables

    def reset(self, schema: Schema) -> None:
        """Reset the builder with a new schema."""
        self.schema = schema
        self.metadata = MetaData(schema=self.schema.name)
        self._objects = {}

    def build(self) -> None:
        """Build the SQA tables and constraints from the schema."""
        self.build_tables()
        self.build_constraints()

    def build_tables(self) -> None:
        """Build the SQA tables from the schema.

        Notes
        -----
        This is the main function for building the SQA tables from the schema
        including objects within tables such as constraints, primary keys,
        and indices, which have their own dedicated sub-functions.
        """
        for table in self.schema.tables:
            self.build_table(table)
            if table.primaryKey:
                primary_key = self.build_primary_key(table.primaryKey)
                self._objects[table.id].append_constraint(primary_key)

    def build_primary_key(self, primary_key_columns: str | list[str]) -> PrimaryKeyConstraint:
        """Build a SQA `PrimaryKeyConstraint` from a single column ID or a list
        or them.

        Parameters
        ----------
        primary_key_columns : `str` or `list` of `str`
            The column ID or list of column IDs from which to build the primary
            key.
        """
        columns: list[Column] = []
        if isinstance(primary_key_columns, str):
            columns.append(self._objects[primary_key_columns])
        else:
            columns.extend([self._objects[column_id] for column_id in primary_key_columns])
        return PrimaryKeyConstraint(*columns)

    def build_table(self, table_obj: dm.Table) -> None:
        """Build a SQA table from a `datamodel.Table` object and it to
        `MetaData` object.

        Parameters
        ----------
        table_obj : `felis.datamodel.Table`
            The table object to build the SQA table from.
        """
        # Process mysql table options.
        optargs = {}
        if table_obj.mysql_engine:
            optargs["mysql_engine"] = table_obj.mysql_engine
        if table_obj.mysql_charset:
            optargs["mysql_charset"] = table_obj.mysql_charset

        # Create the SQA table object and its columns.
        name = table_obj.name
        id = table_obj.id
        description = table_obj.description
        columns = [self.build_column(column) for column in table_obj.columns]
        table = Table(
            name,
            self.metadata,
            *columns,
            comment=description,
            schema=self.schema.name if self.apply_schema_to_tables else None,
            **optargs,  # type: ignore[arg-type]
        )

        # Create the indexes and add them to the table.
        indexes = [self.build_index(index) for index in table_obj.indexes]
        for index in indexes:
            index._set_parent(table)
            table.indexes.add(index)

        self._objects[id] = table

    def build_column(self, column_obj: dm.Column) -> Column:
        """Build a SQA column from a `felis.datamodel.Column` object.

        Parameters
        ----------
        column_obj : `felis.datamodel.Column`
            The column object from which to build the SQA column.

        Returns
        -------
        column: `Column`
            The SQA column object.
        """
        # Get basic column attributes.
        name = column_obj.name
        id = column_obj.id
        datatype_name: str = column_obj.datatype  # type: ignore[assignment]
        description = column_obj.description
        default = column_obj.value
        length = column_obj.length

        # Handle variant overrides based on code from Felis `sql` module.
        variant_dict = make_variant_dict(column_obj)
        felis_type = FelisType.felis_type(datatype_name)
        datatype_fun = getattr(sqltypes, datatype_name)
        if felis_type.is_sized:
            datatype = datatype_fun(length, **variant_dict)
        else:
            datatype = datatype_fun(**variant_dict)

        # Set default value of nullable based on column type and then whether
        # it was explicitly provided in the schema data.
        nullable_default = False if isinstance(datatype, Numeric) else True
        nullable = column_obj.nullable if column_obj.nullable is not None else nullable_default

        # Set autoincrement depending on if it was provided explicitly.
        autoincrement: Literal["auto", "ignore_fk"] | bool = (
            column_obj.autoincrement if column_obj.autoincrement else "auto"
        )

        column: Column = Column(
            name,
            datatype,
            comment=description,
            autoincrement=autoincrement,
            nullable=nullable,
            server_default=default,
        )

        self._objects[id] = column

        return column

    def build_constraints(self) -> None:
        """Build the SQA constraints in the Felis schema and append them to the
        associated `Table`.
        """
        for table_obj in self.schema.tables:
            table = self._objects[table_obj.id]
            for constraint_obj in table_obj.constraints:
                constraint = self.build_constraint(constraint_obj)
                table.append_constraint(constraint)

    def build_constraint(self, constraint_obj: dm.Constraint) -> Constraint:
        """Build a SQA `Constraint` from a `felis.datamodel.Constraint` object.

        Parameters
        ----------
        constraint_obj : `felis.datamodel.Constraint`
            The constraint object from which to build the SQA constraint.

        Returns
        -------
        constraint: `Constraint`
            The SQA constraint object.
        """
        args: dict[str, Any] = {
            "name": constraint_obj.name if constraint_obj.name else None,
            "info": constraint_obj.description if constraint_obj.description else None,
            "deferrable": constraint_obj.deferrable if constraint_obj.deferrable else None,
            "initially": constraint_obj.initially if constraint_obj.initially else None,
        }
        constraint: Constraint
        constraint_type = constraint_obj.type

        if constraint_type == "ForeignKey":
            if isinstance(constraint_obj, dm.ForeignKeyConstraint):
                fk_obj: dm.ForeignKeyConstraint = constraint_obj
                columns = [self._objects[column_id] for column_id in fk_obj.columns]
                refcolumns = [self._objects[column_id] for column_id in fk_obj.referenced_columns]
                constraint = ForeignKeyConstraint(columns, refcolumns, **args)
            else:
                raise TypeError("Unexpected constraint type for ForeignKey: ", type(constraint_obj))
        elif constraint_type == "Check":
            if isinstance(constraint_obj, dm.CheckConstraint):
                check_obj: dm.CheckConstraint = constraint_obj
                expression = check_obj.expression
                constraint = CheckConstraint(expression, **args)
            else:
                raise TypeError("Unexpected constraint type for CheckConstraint: ", type(constraint_obj))
        elif constraint_type == "Unique":
            if isinstance(constraint_obj, dm.UniqueConstraint):
                uniq_obj: dm.UniqueConstraint = constraint_obj
                columns = [self._objects[column_id] for column_id in uniq_obj.columns]
                constraint = UniqueConstraint(*columns, **args)
            else:
                raise TypeError("Unexpected constraint type for UniqueConstraint: ", type(constraint_obj))
        else:
            raise ValueError(f"Unexpected constraint type: {constraint_type}")

        self._objects[constraint_obj.id] = constraint

        return constraint

    def build_index(self, index_obj: dm.Index) -> Index:
        """Build a SQA `Index` from a `felis.datamodel.Index` object.

        Parameters
        ----------
        index_obj : `felis.datamodel.Index`
            The index object from which to build the SQA index.

        Returns
        -------
        index: `Index`
            The SQA index object.
        """
        columns = [self._objects[c_id] for c_id in (index_obj.columns if index_obj.columns else [])]
        expressions = index_obj.expressions if index_obj.expressions else []
        index = Index(index_obj.name, *columns, *expressions)
        self._objects[index_obj.id] = index
        return index


class ConnectionWrapper:
    """A wrapper for a SQLAlchemy engine or mock connection which provides a
    consistent interface for executing SQL statements.
    """

    def __init__(self, engine: Engine | MockConnection):
        """Initialize the connection wrapper.

        Parameters
        ----------
        engine : `Engine` or `MockConnection`
            The SQLAlchemy engine or mock connection to wrap.
        """
        self.engine = engine

    def execute(self, statement: Any) -> ResultProxy:
        """Execute a SQL statement on the engine and return the result."""
        if isinstance(statement, str):
            statement = text(statement)
        if isinstance(self.engine, MockConnection):
            return self.engine.connect().execute(statement)
        else:
            with self.engine.connect() as connection:
                result = connection.execute(statement)
                connection.commit()
                return result


class DatabaseContext:
    """A class for managing the schema and its database connection."""

    def __init__(self, metadata: MetaData, engine: Engine | MockConnection):
        """Initialize the database context.

        Parameters
        ----------
        metadata : `MetaData`
            The SQLAlchemy metadata object.

        engine : `Engine` or `MockConnection`
            The SQLAlchemy engine or mock connection object.
        """
        self.engine = engine
        self.metadata = metadata
        self.connection = ConnectionWrapper(engine)

    def create_if_not_exists(self) -> None:
        """Create the schema in the database if it does not exist.

        In MySQL, this will create a new database. In PostgreSQL, it will
        create a new schema. For other variants, this is an unsupported
        operation.

        Parameters
        ----------
        engine: `Engine`
            The SQLAlchemy engine object.
        schema_name: `str`
            The name of the schema (or database) to create.
        """
        db_type = self.engine.dialect.name
        schema_name = self.metadata.schema
        try:
            if db_type == "mysql":
                logger.info(f"Creating MySQL database: {schema_name}")
                self.connection.execute(text(f"CREATE DATABASE IF NOT EXISTS {schema_name}"))
            elif db_type == "postgresql":
                logger.info(f"Creating PG schema: {schema_name}")
                self.connection.execute(sqa_schema.CreateSchema(schema_name, if_not_exists=True))
            else:
                raise ValueError("Unsupported database type:" + db_type)
        except SQLAlchemyError as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def drop_if_exists(self) -> None:
        """Drop the schema in the database if it exists.

        In MySQL, this will drop a database. In PostgreSQL, it will drop a
        schema. For other variants, this is unsupported for now.

        Parameters
        ----------
        engine: `Engine`
            The SQLAlchemy engine object.
        schema_name: `str`
            The name of the schema (or database) to drop.
        """
        db_type = self.engine.dialect.name
        schema_name = self.metadata.schema
        try:
            if db_type == "mysql":
                logger.info(f"Dropping MySQL database if exists: {schema_name}")
                self.connection.execute(text(f"DROP DATABASE IF EXISTS {schema_name}"))
            elif db_type == "postgresql":
                logger.info(f"Dropping PostgreSQL schema if exists: {schema_name}")
                self.connection.execute(sqa_schema.DropSchema(schema_name, if_exists=True))
            else:
                raise ValueError("Unsupported database type:" + db_type)
        except SQLAlchemyError as e:
            logger.error(f"Error dropping schema: {e}")
            raise

    def create_all(self) -> None:
        """Create all tables in the schema using the metadata object."""
        self.metadata.create_all(self.engine)

    @staticmethod
    def create_mock_engine(engine_url: URL, output_file: TextIOBase | None = None) -> MockConnection:
        """Create a mock engine for testing or dumping DDL statements.

        Parameters
        ----------
        engine_url : `URL`
            The SQLAlchemy engine URL.

        output_file : `TextIOBase` or None, optional
            The file to write the SQL statements to. If None, the statements
            will be written to stdout.
        """
        dumper = InsertDump(output_file)
        engine = create_mock_engine(make_url(engine_url), executor=dumper.dump)
        dumper.dialect = engine.dialect
        return engine
