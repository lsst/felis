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
from typing import IO, Any, Literal

import sqlalchemy.schema as sqa_schema
from lsst.utils.iteration import ensure_iterable
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
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.engine.mock import MockConnection
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import TypeEngine

from felis.datamodel import Schema
from felis.db._variants import make_variant_dict

from . import datamodel
from .db import sqltypes
from .types import FelisType

logger = logging.getLogger(__name__)


class InsertDump:
    """An Insert Dumper for SQL statements which supports writing messages
    to stdout or a file.
    """

    def __init__(self, file: IO[str] | None = None) -> None:
        """Initialize the insert dumper.

        Parameters
        ----------
        file : `io.TextIOBase` or `None`, optional
            The file to write the SQL statements to. If None, the statements
            will be written to stdout.
        """
        self.file = file
        self.dialect: Dialect | None = None

    def dump(self, sql: Any, *multiparams: Any, **params: Any) -> None:
        """Dump the SQL statement to a file or stdout.

        Statements with parameters will be formatted with the values
        inserted into the resultant SQL output.

        Parameters
        ----------
        sql : `typing.Any`
            The SQL statement to dump.
        multiparams : `typing.Any`
            The multiparams to use for the SQL statement.
        params : `typing.Any`
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


def get_datatype_with_variants(column_obj: datamodel.Column) -> TypeEngine:
    """Use the Felis type system to get a SQLAlchemy datatype with variant
    overrides from the information in a `Column` object.

    Parameters
    ----------
    column_obj : `felis.datamodel.Column`
        The column object from which to get the datatype.

    Raises
    ------
    ValueError
        If the column has a sized type but no length.
    """
    variant_dict = make_variant_dict(column_obj)
    felis_type = FelisType.felis_type(column_obj.datatype.value)
    datatype_fun = getattr(sqltypes, column_obj.datatype.value)
    if felis_type.is_sized:
        if not column_obj.length:
            raise ValueError(f"Column {column_obj.name} has sized type '{column_obj.datatype}' but no length")
        datatype = datatype_fun(column_obj.length, **variant_dict)
    else:
        datatype = datatype_fun(**variant_dict)
    return datatype


class MetaDataBuilder:
    """A class for building a `MetaData` object from a Felis `Schema`."""

    def __init__(
        self, schema: Schema, apply_schema_to_metadata: bool = True, apply_schema_to_tables: bool = True
    ) -> None:
        """Initialize the metadata builder.

        Parameters
        ----------
        schema : `felis.datamodel.Schema`
            The schema object from which to build the SQLAlchemy metadata.
        apply_schema_to_metadata : `bool`, optional
            Whether to apply the schema name to the metadata object.
        apply_schema_to_tables : `bool`, optional
            Whether to apply the schema name to the tables.
        """
        self.schema = schema
        if not apply_schema_to_metadata:
            logger.debug("Schema name will not be applied to metadata")
        if not apply_schema_to_tables:
            logger.debug("Schema name will not be applied to tables")
        self.metadata = MetaData(schema=schema.name if apply_schema_to_metadata else None)
        self._objects: dict[str, Any] = {}
        self.apply_schema_to_tables = apply_schema_to_tables

    def build(self) -> MetaData:
        """Build the SQLAlchemy tables and constraints from the schema."""
        self.build_tables()
        self.build_constraints()
        return self.metadata

    def build_tables(self) -> None:
        """Build the SQLAlchemy tables from the schema.

        Notes
        -----
        This function builds all the tables by calling ``build_table`` on
        each Pydantic object. It also calls ``build_primary_key`` to create the
        primary key constraints.
        """
        for table in self.schema.tables:
            self.build_table(table)
            if table.primary_key:
                primary_key = self.build_primary_key(table.primary_key)
                self._objects[table.id].append_constraint(primary_key)

    def build_primary_key(self, primary_key_columns: str | list[str]) -> PrimaryKeyConstraint:
        """Build a SQLAlchemy `PrimaryKeyConstraint` from a single column ID
        or a list.

        The `primary_key_columns` are strings or a list of strings representing
        IDs pointing to columns that will be looked up in the internal object
        dictionary.

        Parameters
        ----------
        primary_key_columns : `str` or `list` of `str`
            The column ID or list of column IDs from which to build the primary
            key.

        Returns
        -------
        primary_key: `sqlalchemy.PrimaryKeyConstraint`
            The SQLAlchemy primary key constraint object.
        """
        return PrimaryKeyConstraint(
            *[self._objects[column_id] for column_id in ensure_iterable(primary_key_columns)]
        )

    def build_table(self, table_obj: datamodel.Table) -> None:
        """Build a `sqlalchemy.Table` from a `felis.datamodel.Table` and add
        it to the `sqlalchemy.MetaData` object.

        Several MySQL table options are handled by annotations on the table,
        including the engine and charset. This is not needed for Postgres,
        which does not have equivalent options.

        Parameters
        ----------
        table_obj : `felis.datamodel.Table`
            The table object to build the SQLAlchemy table from.
        """
        # Process mysql table options.
        optargs = {}
        if table_obj.mysql_engine:
            optargs["mysql_engine"] = table_obj.mysql_engine
        if table_obj.mysql_charset:
            optargs["mysql_charset"] = table_obj.mysql_charset

        # Create the SQLAlchemy table object and its columns.
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

    def build_column(self, column_obj: datamodel.Column) -> Column:
        """Build a SQLAlchemy column from a `felis.datamodel.Column` object.

        Parameters
        ----------
        column_obj : `felis.datamodel.Column`
            The column object from which to build the SQLAlchemy column.

        Returns
        -------
        column: `sqlalchemy.Column`
            The SQLAlchemy column object.
        """
        # Get basic column attributes.
        name = column_obj.name
        id = column_obj.id
        description = column_obj.description
        default = column_obj.value

        # Handle variant overrides for the column (e.g., "mysql:datatype").
        datatype = get_datatype_with_variants(column_obj)

        # Set default value of nullable based on column type and then whether
        # it was explicitly provided in the schema data.
        nullable = column_obj.nullable
        if nullable is None:
            nullable = False if isinstance(datatype, Numeric) else True

        # Set autoincrement depending on if it was provided explicitly.
        autoincrement: Literal["auto"] | bool = (
            column_obj.autoincrement if column_obj.autoincrement is not None else "auto"
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
        """Build the SQLAlchemy constraints in the Felis schema and append them
        to the associated `Table`.

        Notes
        -----
        This is performed as a separate step after building the tables so that
        all the referenced objects in the constraints will be present and can
        be looked up by their ID.
        """
        for table_obj in self.schema.tables:
            table = self._objects[table_obj.id]
            for constraint_obj in table_obj.constraints:
                constraint = self.build_constraint(constraint_obj)
                table.append_constraint(constraint)

    def build_constraint(self, constraint_obj: datamodel.Constraint) -> Constraint:
        """Build a SQLAlchemy `Constraint` from a `felis.datamodel.Constraint`
        object.

        Parameters
        ----------
        constraint_obj : `felis.datamodel.Constraint`
            The constraint object from which to build the SQLAlchemy
            constraint.

        Returns
        -------
        constraint: `sqlalchemy.Constraint`
            The SQLAlchemy constraint object.

        Raises
        ------
        ValueError
            If the constraint type is not recognized.
        TypeError
            If the constraint object is not the expected type.
        """
        args: dict[str, Any] = {
            "name": constraint_obj.name or None,
            "info": constraint_obj.description or None,
            "deferrable": constraint_obj.deferrable or None,
            "initially": constraint_obj.initially or None,
        }
        constraint: Constraint
        constraint_type = constraint_obj.type

        if isinstance(constraint_obj, datamodel.ForeignKeyConstraint):
            fk_obj: datamodel.ForeignKeyConstraint = constraint_obj
            columns = [self._objects[column_id] for column_id in fk_obj.columns]
            refcolumns = [self._objects[column_id] for column_id in fk_obj.referenced_columns]
            constraint = ForeignKeyConstraint(columns, refcolumns, **args)
        elif isinstance(constraint_obj, datamodel.CheckConstraint):
            check_obj: datamodel.CheckConstraint = constraint_obj
            expression = check_obj.expression
            constraint = CheckConstraint(expression, **args)
        elif isinstance(constraint_obj, datamodel.UniqueConstraint):
            uniq_obj: datamodel.UniqueConstraint = constraint_obj
            columns = [self._objects[column_id] for column_id in uniq_obj.columns]
            constraint = UniqueConstraint(*columns, **args)
        else:
            raise ValueError(f"Unknown constraint type: {constraint_type}")

        self._objects[constraint_obj.id] = constraint

        return constraint

    def build_index(self, index_obj: datamodel.Index) -> Index:
        """Build a SQLAlchemy `Index` from a `felis.datamodel.Index` object.

        Parameters
        ----------
        index_obj : `felis.datamodel.Index`
            The index object from which to build the SQLAlchemy index.

        Returns
        -------
        index: `sqlalchemy.Index`
            The SQLAlchemy index object.
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
        engine : `sqlalchemy.Engine` or `sqlalchemy.MockConnection`
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
            with self.engine.begin() as connection:
                result = connection.execute(statement)
                return result


class DatabaseContext:
    """A class for managing the schema and its database connection."""

    def __init__(self, metadata: MetaData, engine: Engine | MockConnection):
        """Initialize the database context.

        Parameters
        ----------
        metadata : `sqlalchemy.MetaData`
            The SQLAlchemy metadata object.

        engine : `sqlalchemy.Engine` or `sqlalchemy.MockConnection`
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
        engine: `sqlalchemy.Engine`
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
        engine: `sqlalchemy.Engine`
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
                raise ValueError(f"Unsupported database type: {db_type}")
        except SQLAlchemyError as e:
            logger.error(f"Error dropping schema: {e}")
            raise

    def create_all(self) -> None:
        """Create all tables in the schema using the metadata object."""
        self.metadata.create_all(self.engine)

    @staticmethod
    def create_mock_engine(engine_url: URL, output_file: IO[str] | None = None) -> MockConnection:
        """Create a mock engine for testing or dumping DDL statements.

        Parameters
        ----------
        engine_url : `sqlalchemy.engine.url.URL`
            The SQLAlchemy engine URL.
        output_file : `typing.IO` [ `str` ] or `None`, optional
            The file to write the SQL statements to. If None, the statements
            will be written to stdout.
        """
        dumper = InsertDump(output_file)
        engine = create_mock_engine(make_url(engine_url), executor=dumper.dump)
        dumper.dialect = engine.dialect
        return engine
