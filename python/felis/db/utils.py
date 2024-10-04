"""Database utility functions and classes."""

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
import re
from typing import IO, Any

from sqlalchemy import MetaData, types
from sqlalchemy.engine import Dialect, Engine, ResultProxy
from sqlalchemy.engine.mock import MockConnection, create_mock_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.sql import text
from sqlalchemy.types import TypeEngine

from .dialects import get_dialect_module

__all__ = ["string_to_typeengine", "SQLWriter", "ConnectionWrapper", "DatabaseContext"]

logger = logging.getLogger("felis")

_DATATYPE_REGEXP = re.compile(r"(\w+)(\((.*)\))?")
"""Regular expression to match data types with parameters in parentheses."""


def string_to_typeengine(
    type_string: str, dialect: Dialect | None = None, length: int | None = None
) -> TypeEngine:
    """Convert a string representation of a datatype to a SQLAlchemy type.

    Parameters
    ----------
    type_string
        The string representation of the data type.
    dialect
        The SQLAlchemy dialect to use. If None, the default dialect will be
        used.
    length
        The length of the data type. If the data type does not have a length
        attribute, this parameter will be ignored.

    Returns
    -------
    `sqlalchemy.types.TypeEngine`
        The SQLAlchemy type engine object.

    Raises
    ------
    ValueError
        Raised if the type string is invalid or the type is not supported.

    Notes
    -----
    This function is used when converting type override strings defined in
    fields such as ``mysql:datatype`` in the schema data.
    """
    match = _DATATYPE_REGEXP.search(type_string)
    if not match:
        raise ValueError(f"Invalid type string: {type_string}")

    type_name, _, params = match.groups()
    if dialect is None:
        type_class = getattr(types, type_name.upper(), None)
    else:
        try:
            dialect_module = get_dialect_module(dialect.name)
        except KeyError:
            raise ValueError(f"Unsupported dialect: {dialect}")
        type_class = getattr(dialect_module, type_name.upper(), None)

    if not type_class:
        raise ValueError(f"Unsupported type: {type_class}")

    if params:
        params = [int(param) if param.isdigit() else param for param in params.split(",")]
        type_obj = type_class(*params)
    else:
        type_obj = type_class()

    if hasattr(type_obj, "length") and getattr(type_obj, "length") is None and length is not None:
        type_obj.length = length

    return type_obj


def is_mock_url(url: URL) -> bool:
    """Check if the engine URL is a mock URL.

    Parameters
    ----------
    url
        The SQLAlchemy engine URL.

    Returns
    -------
    bool
        True if the URL is a mock URL, False otherwise.
    """
    return (url.drivername == "sqlite" and url.database is None) or (
        url.drivername != "sqlite" and url.host is None
    )


def is_valid_engine(engine: Engine | MockConnection | None) -> bool:
    """Check if the engine is valid.

    The engine cannot be none; it must not be a mock connection; and it must
    not be a mock URL which is missing a host or, for sqlite, a database name.

    Parameters
    ----------
    engine
        The SQLAlchemy engine or mock connection.

    Returns
    -------
    bool
        True if the engine is valid, False otherwise.
    """
    return engine is not None and not isinstance(engine, MockConnection) and not is_mock_url(engine.url)


class SQLWriter:
    """Write SQL statements to stdout or a file.

    Parameters
    ----------
    file
        The file to write the SQL statements to. If None, the statements
        will be written to stdout.
    """

    def __init__(self, file: IO[str] | None = None) -> None:
        """Initialize the SQL writer."""
        self.file = file
        self.dialect: Dialect | None = None

    def write(self, sql: Any, *multiparams: Any, **params: Any) -> None:
        """Write the SQL statement to a file or stdout.

        Statements with parameters will be formatted with the values
        inserted into the resultant SQL output.

        Parameters
        ----------
        sql
            The SQL statement to write.
        *multiparams
            The multiparams to use for the SQL statement.
        **params
            The params to use for the SQL statement.

        Notes
        -----
        The functions arguments are typed very loosely because this method in
        SQLAlchemy is untyped, amd we do not call it directly.
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


class ConnectionWrapper:
    """Wrap a SQLAlchemy engine or mock connection to provide a consistent
    interface for executing SQL statements.

    Parameters
    ----------
    engine
        The SQLAlchemy engine or mock connection to wrap.
    """

    def __init__(self, engine: Engine | MockConnection):
        """Initialize the connection wrapper."""
        self.engine = engine

    def execute(self, statement: Any) -> ResultProxy:
        """Execute a SQL statement on the engine and return the result.

        Parameters
        ----------
        statement
            The SQL statement to execute.

        Returns
        -------
        ``sqlalchemy.engine.ResultProxy``
            The result of the statement execution.

        Notes
        -----
        The statement will be executed in a transaction block if not using
        a mock connection.
        """
        if isinstance(statement, str):
            statement = text(statement)
        if isinstance(self.engine, Engine):
            try:
                with self.engine.begin() as connection:
                    result = connection.execute(statement)
                    return result
            except SQLAlchemyError as e:
                connection.rollback()
                logger.error(f"Error executing statement: {e}")
                raise
        elif isinstance(self.engine, MockConnection):
            return self.engine.connect().execute(statement)
        else:
            raise ValueError("Unsupported engine type:" + str(type(self.engine)))


class DatabaseContext:
    """Manage the database connection and SQLAlchemy metadata.

    Parameters
    ----------
    metadata
        The SQLAlchemy metadata object.

    engine
        The SQLAlchemy engine or mock connection object.
    """

    def __init__(self, metadata: MetaData, engine: Engine | MockConnection):
        """Initialize the database context."""
        self.engine = engine
        self.dialect_name = engine.dialect.name
        self.metadata = metadata
        self.connection = ConnectionWrapper(engine)

    def initialize(self) -> None:
        """Create the schema in the database if it does not exist.

        Raises
        ------
        ValueError
            Raised if the database is not supported or it already exists.
        sqlalchemy.exc.SQLAlchemyError
            Raised if there is an error creating the schema.

        Notes
        -----
        In MySQL, this will create a new database and, in PostgreSQL, it will
        create a new schema. For other variants, this is an unsupported
        operation.
        """
        schema_name = self.metadata.schema
        try:
            if self.dialect_name == "mysql":
                logger.debug(f"Checking if MySQL database exists: {schema_name}")
                result = self.execute(text(f"SHOW DATABASES LIKE '{schema_name}'"))
                if result.fetchone():
                    raise ValueError(f"MySQL database '{schema_name}' already exists.")
                logger.debug(f"Creating MySQL database: {schema_name}")
                self.execute(text(f"CREATE DATABASE {schema_name}"))
            elif self.dialect_name == "postgresql":
                logger.debug(f"Checking if PG schema exists: {schema_name}")
                result = self.execute(
                    text(
                        f"""
                        SELECT schema_name
                        FROM information_schema.schemata
                        WHERE schema_name = '{schema_name}'
                        """
                    )
                )
                if result.fetchone():
                    raise ValueError(f"PostgreSQL schema '{schema_name}' already exists.")
                logger.debug(f"Creating PG schema: {schema_name}")
                self.execute(CreateSchema(schema_name))
            elif self.dialect_name == "sqlite":
                # Just silently ignore this operation for SQLite. The database
                # will still be created if it does not exist and the engine
                # URL is valid.
                pass
            else:
                raise ValueError(f"Initialization not supported for: {self.dialect_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error creating schema: {e}")
            raise

    def drop(self) -> None:
        """Drop the schema in the database if it exists.

        Raises
        ------
        ValueError
            Raised if the database is not supported.

        Notes
        -----
        In MySQL, this will drop a database. In PostgreSQL, it will drop a
        schema. For other variants, this is an unsupported operation.
        """
        schema_name = self.metadata.schema
        if not self.engine.dialect.name == "sqlite" and self.metadata.schema is None:
            raise ValueError("Schema name is required to drop the schema.")
        try:
            if self.dialect_name == "mysql":
                logger.debug(f"Dropping MySQL database if exists: {schema_name}")
                self.execute(text(f"DROP DATABASE IF EXISTS {schema_name}"))
            elif self.dialect_name == "postgresql":
                logger.debug(f"Dropping PostgreSQL schema if exists: {schema_name}")
                self.execute(DropSchema(schema_name, if_exists=True, cascade=True))
            elif self.dialect_name == "sqlite":
                if isinstance(self.engine, Engine):
                    logger.debug("Dropping tables in SQLite schema")
                    self.metadata.drop_all(bind=self.engine)
            else:
                raise ValueError(f"Drop operation not supported for: {self.dialect_name}")
        except SQLAlchemyError as e:
            logger.error(f"Error dropping schema: {e}")
            raise

    def create_all(self) -> None:
        """Create all tables in the schema using the metadata object."""
        if isinstance(self.engine, Engine):
            # Use a transaction for a real connection.
            with self.engine.begin() as conn:
                try:
                    self.metadata.create_all(bind=conn)
                    conn.commit()
                except SQLAlchemyError as e:
                    conn.rollback()
                    logger.error(f"Error creating tables: {e}")
                    raise
        elif isinstance(self.engine, MockConnection):
            # Mock connection so no need for a transaction.
            self.metadata.create_all(self.engine)
        else:
            raise ValueError("Unsupported engine type: " + str(type(self.engine)))

    @staticmethod
    def create_mock_engine(engine_url: str | URL, output_file: IO[str] | None = None) -> MockConnection:
        """Create a mock engine for testing or dumping DDL statements.

        Parameters
        ----------
        engine_url
            The SQLAlchemy engine URL.
        output_file
            The file to write the SQL statements to. If None, the statements
            will be written to stdout.

        Returns
        -------
        ``sqlalchemy.engine.mock.MockConnection``
            The mock connection object.
        """
        writer = SQLWriter(output_file)
        engine = create_mock_engine(engine_url, executor=writer.write, paramstyle="pyformat")
        writer.dialect = engine.dialect
        return engine

    def execute(self, statement: Any) -> ResultProxy:
        """Execute a SQL statement on the engine and return the result.

        Parameters
        ----------
        statement
            The SQL statement to execute.

        Returns
        -------
        ``sqlalchemy.engine.ResultProxy``
            The result of the statement execution.

        Notes
        -----
        This is just a wrapper around the execution method of the connection
        object, which may execute on a real or mock connection.
        """
        return self.connection.execute(statement)
