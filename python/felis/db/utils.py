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

from __future__ import annotations
import logging
from typing import IO, Any
from sqlalchemy.engine import Dialect, Engine, ResultProxy
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine.url import URL
from sqlalchemy.schema import CreateSchema, DropSchema
from sqlalchemy.engine.mock import MockConnection, create_mock_engine
from sqlalchemy import MetaData, make_url

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
                self.connection.execute(CreateSchema(schema_name, if_not_exists=True))
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
                self.connection.execute(DropSchema(schema_name, if_exists=True, cascade=True))
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